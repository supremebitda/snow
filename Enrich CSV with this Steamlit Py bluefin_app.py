import streamlit as st
import pandas as pd
import numpy as np

def warehouse_breakdown(temp):
    days_in_set = len(temp.drop_duplicates(['MONTH', 'DAY', 'HR']))/24
    # look up table for snowflake warehouse size to credit cost
    credits_ph = {"X-Small":1, "Small":2, "Medium":4, "Large":8, "X-Large":16, "2X-Large":32, "3X-Large":64, "4X-Large": 128, "5X-Large":256, "6X-Large": 512}
    # this counter will be summed to show how many minutes a warehouse was running. snowflake bills by the minute so this gives us most granular cost information
    temp['MN_COUNTER'] = 1
    # look up the credit cost for each warehouse
    temp['CREDITS_PER_HOUR'] = temp['WAREHOUSE_SIZE'].apply(lambda x: credits_ph[x])
    # aggregate the output by warehouse
    temp = temp[['WAREHOUSE_SIZE', 'UNIQUE_WH_NAME','CREDITS_PER_HOUR', 'QUERY_COUNT', 'IS_READ', 'IS_WRITE','READ_TIME', 'WRITE_TIME', 'MN_COUNTER']].groupby(['WAREHOUSE_SIZE','UNIQUE_WH_NAME', 'CREDITS_PER_HOUR']).sum().reset_index()
    temp['DAYS_IN_SET'] = days_in_set

    # Calculate the enrichment columns

    temp['HOURS_PER_DAY'] = (temp['MN_COUNTER']/60)/temp['DAYS_IN_SET']
    temp['CREDITS_IN_SET'] = temp['CREDITS_PER_HOUR']*(temp['MN_COUNTER']/60)
    temp['CREDITS_PER_DAY'] = temp['CREDITS_IN_SET'] / temp['DAYS_IN_SET']

    credit_cost = 3

    temp['$_PER_DAY'] = temp['CREDITS_PER_DAY'] * credit_cost
    temp['$_PER_MONTH'] = temp['$_PER_DAY'] * 30.437 #30.437 is the average number of days in a month
    temp['$_PER_YEAR'] = temp['$_PER_DAY'] * 365 

    temp['WRITE_RATIO'] = temp['WRITE_TIME']/(temp['READ_TIME']+temp['WRITE_TIME'])

    # Calculate the number of credits needed to run the remaining workload after writes have been offloaded
    temp['REMAINING_CREDITS_PER_DAY'] = temp['CREDITS_PER_DAY']*(1-temp['WRITE_RATIO'])
    # After offloading writes, we can probably downsize the warehouse by one size, unless its an extra small
    temp['REMAINING_SIZE'] = np.where((temp['CREDITS_PER_HOUR'] < 1) & (temp['WRITE_RATIO'] > .5), pd.Series(temp['REMAINING_CREDITS_PER_DAY']/2), pd.Series(temp['REMAINING_CREDITS_PER_DAY']))
    #If we're not spending more than 5 credits a day on the warehouse, we can probably get rid of it all together and consolidate that workload into another warehouse
    temp['REMAINING_COUNT'] = np.where((temp['REMAINING_SIZE'] < 5) & (temp['WRITE_RATIO'] > 0.5), 0, pd.Series(temp['REMAINING_SIZE']))


    temp['REMAINING_$_PER_DAY'] = temp['REMAINING_COUNT'] * credit_cost
    temp['REMAINING_$_PER_MONTH'] = temp['REMAINING_$_PER_DAY'] * 30.437
    temp['REMAINING_$_PER_YEAR'] = temp['REMAINING_$_PER_DAY'] * 365
    temp['SAVINGS'] = temp['$_PER_YEAR'] - temp['REMAINING_$_PER_YEAR'] 
    temp['REDUCTION'] = (temp['$_PER_YEAR'] - temp['REMAINING_$_PER_YEAR'])/temp['$_PER_YEAR']

    total_daily_spend = temp['$_PER_DAY'].sum()
    temp['%_OF_SPEND'] = temp['$_PER_DAY'] / total_daily_spend

    # Rank which warehouses to consider performing this offload on based on what percentage the warehouse is of the total snowflake spend,
    # how often the warehouse is running, and how write oriented the warehouse is
    temp['COST_RANK'] = temp['%_OF_SPEND'].rank(method='min', ascending=False)

    temp['FREQUENCY_RANK'] = temp['MN_COUNTER'].rank(method='min', ascending=False)

    temp['INEFFICIENY_RANK'] = temp['WRITE_RATIO'].rank(method='min', ascending=False)

    temp['SCORE'] = temp['COST_RANK']*3 + temp['FREQUENCY_RANK'] + temp['INEFFICIENY_RANK']
    temp['SCRUTINY_RANK'] = temp['SCORE'].rank(method='min')


    temp = temp.sort_values(by='SCRUTINY_RANK', ignore_index=True)
    return temp

@st.cache_data
def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode("utf-8")


def on_upload():
    uploaded_file = st.session_state['uploaded_file']
    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)
        st.session_state['dataframe'] = warehouse_breakdown(df)



if "dataframe" not in st.session_state:
    st.session_state["dataframe"] = pd.DataFrame()

st.title("Bluefin")


f = st.file_uploader("Upload Output CSV", key="uploaded_file", on_change=on_upload)
if not st.session_state["dataframe"].empty:
    st.dataframe(st.session_state["dataframe"])
    csv = convert_df(st.session_state["dataframe"])
    
    filename = st.text_input("Output File Name", "warehouse_breakdown")
    st.download_button(
    label="Download data as CSV",
    data=csv,
    file_name=f"{filename}.csv",
    mime="text/csv",
)


