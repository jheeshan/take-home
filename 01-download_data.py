import os
import pathlib

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

load_dotenv()
#set paths for file navigation
BASE_DIR = pathlib.Path().resolve().parent
DATA_DIR = BASE_DIR / 'data'
RAW_DIR = DATA_DIR / 'raw'
PROCESSED_DIR = DATA_DIR / 'processed'

#load credentials for connecting to GCP
SERVICE_ACCOUNT_FILE = os.getenv('SERVICE_ACCOUNT_FILE')
PROJECT_ID = os.getenv('PROJECT_ID')
creds = service_account.Credentials.from_service_account_file(BASE_DIR / SERVICE_ACCOUNT_FILE)

#instantiate BQ client
client = bigquery.Client(credentials=creds, project=PROJECT_ID)



#functions to download datasets
def get_monthly_charges_2021(target_file_name: str):
    """
    Connects to monthly_charges_2021 table on BQ, loads data into a pandas dataframe 
    and writes to a csv file.
       
    Args:
        target_file_name (str): Input the name you wish the generated CSV to take.
    """
    sql = """
        SELECT *
        FROM `analytics-219613.toggl_take_home_data.monthly_charges_2021`
    """
    
    destination = RAW_DIR / target_file_name
    df = client.query(sql).to_dataframe() 
    df.to_csv(destination, index=False, header=True)
    print(f'CSV file created. File name is {target_file_name} and location is {RAW_DIR}')
    

def get_orgs_before_2022(target_file_name: str):
    """
    Connects to organizations_before_2022 table on BQ, loads relevant columns into a pandas dataframe 
    and writes to a csv file.

    Args:
        target_file_name (str): Input the name you wish the generated CSV to take.
    """
    sql = """
        SELECT 
            organization_id,
            organization_owner_id,
            created_at,
            has_active_subscription,
            current_subscription_plan,
            current_billing_period,
            first_billed_user_count,
            revenue_realized_to_date_usd,
            time_entries_count,
            billable_time_entries_count,
            hours_tracked,
            billable_hours_tracked,
            clients_used,
            projects_used,
            billable_projects_used,
            country,
            industry,
            approximate_employees,
            reported_annual_revenue
        FROM `analytics-219613.toggl_take_home_data.organizations_before_2022`
    """
    
    destination = RAW_DIR / target_file_name
    df = client.query(sql).to_dataframe() 
    df.to_csv(destination, index=False, header=True)
    print(f'CSV file created. File name is {target_file_name} and location is {RAW_DIR}')
    

def get_paying_organizations(target_file_name: str):
    """
    Extracts the list of customers with more than 0 revenue from monthly_charges_2021
    to a csv file.

    Args:
        target_file_name (str): Input the name you wish the generated CSV to take.
    """
    
    sql = """
        WITH nonzero_earners AS (
            SELECT
                organization_id,
                SUM(amount_usd) as total_revenue_2021
            FROM `analytics-219613.toggl_take_home_data.monthly_charges_2021`
            GROUP BY 1
            HAVING SUM(amount_usd) > 0
        )

        SELECT
            a.organization_id,
            b.organization_owner_id,
            a.total_revenue_2021,
            b.revenue_realized_to_date_usd,
            b.time_entries_count,
            b.billable_time_entries_count,
            b.hours_tracked,
            b.billable_hours_tracked,
            b.clients_used,
            b.projects_used,
            b.billable_projects_used,
            b.country,
            b.industry,
            b.company_type,
            b.approximate_employees,
            b.reported_annual_revenue
        FROM nonzero_earners a
        INNER JOIN `analytics-219613.toggl_take_home_data.organizations_before_2022` b
        ON a.organization_id = b.organization_id
        ORDER BY 3 DESC
    """
    
    destination = PROCESSED_DIR / target_file_name
    df = client.query(sql).to_dataframe() 
    df.to_csv(destination, index=False, header=True)
    print(f'CSV file created. File name is {target_file_name} and location is {PROCESSED_DIR}')
    

def get_churn_numbers(target_file_name: str):
    """
    Extracts list of customers, the number of times each one churned,
    reactivated their subscription or was retained, and total revenue for 2021.

    Args:
        target_file_name (str): Input the name you wish the generated CSV to take.
    """
    
    sql = """
        SELECT
            a.organization_id,
            COUNT(CASE WHEN type = 'Churned' THEN 1 ELSE NULL END) as times_churned,
            COUNT(CASE WHEN type = 'Reactivated' THEN 1 ELSE NULL END) as times_reactivated,
            COUNT(CASE WHEN type = 'Retained' THEN 1 ELSE NULL END) as times_retained,
            SUM(amount_usd) as revenue
        FROM `analytics-219613.toggl_take_home_data.monthly_charges_2021` a
        GROUP BY 1
        ORDER BY 2 DESC, 3 ASC
    """
    
    destination = PROCESSED_DIR / target_file_name
    df = client.query(sql).to_dataframe() 
    df.to_csv(destination, index=False, header=True)
    print(f'CSV file created. File name is {target_file_name} and location is {PROCESSED_DIR}')
    

def get_outliers_and_mode(target_file_name: str):
    """
    Extracts list of customers with earnings outside the outlier thresholds,
    and within the mode range.

    Args:
        target_file_name (str): Input the name you wish the generated CSV to take.
    """
    
    sql = """
        WITH outlier_earners AS (
            SELECT
                organization_id,
                SUM(amount_usd) as total_revenue_2021
            FROM `analytics-219613.toggl_take_home_data.monthly_charges_2021`
            GROUP BY 1
            HAVING (
                (SUM(amount_usd) > 22000) OR
                (SUM(amount_usd) BETWEEN 90 AND 250)
            )
        )

        SELECT
            a.organization_id,
            a.total_revenue_2021,
            b.organization_owner_id,
            b.revenue_realized_to_date_usd,
            b.time_entries_count,
            b.billable_time_entries_count,
            b.hours_tracked,
            b.billable_hours_tracked,
            b.clients_used,
            b.projects_used,
            b.billable_projects_used,
            b.country,
            b.industry,
            b.company_type,
            b.approximate_employees,
            b.reported_annual_revenue
        FROM outlier_earners a
        INNER JOIN `analytics-219613.toggl_take_home_data.organizations_before_2022` b
        ON a.organization_id = b.organization_id
        ORDER BY 2 DESC
    """
    
    destination = PROCESSED_DIR / target_file_name
    df = client.query(sql).to_dataframe() 
    df.to_csv(destination, index=False, header=True)
    print(f'CSV file created. File name is {target_file_name} and location is {PROCESSED_DIR}')
    

def get_groupby_plan(target_file_name: str):
    """
    Extracts counts of various metrics, grouped by plan, month, and quarter.

    Args:
        target_file_name (str): Input the name you wish the generated CSV to take.
    """
    
    sql = """
        WITH groupedby_plans AS (
            SELECT
                plan,
                EXTRACT(QUARTER FROM charged_on) as quarter,
                EXTRACT(MONTH FROM charged_on) as month,
                COUNT(*) as num_charges,
                SUM(CASE WHEN is_sales_driven = True THEN 1 ELSE 0 END) as num_sales_driven,
                SUM(amount_usd) as revenue
            FROM `analytics-219613.toggl_take_home_data.monthly_charges_2021`
            GROUP BY 1,2,3 
            ORDER BY 1,2,3
        )


        SELECT
            plan,
            month,
            quarter,
            SUM(num_charges) OVER (PARTITION BY plan, month) as monthly_count,
            SUM(num_charges) OVER (PARTITION BY plan, quarter) as qtr_count,
            SUM(num_charges) OVER (PARTITION BY plan) as count_by_plan,
            SUM(num_charges) OVER () as total_count,
            SUM(num_sales_driven) OVER (PARTITION BY plan, month) as monthly_sales_driven,
            SUM(num_sales_driven) OVER (PARTITION BY plan, quarter) as qtr_sales_driven,
            SUM(num_sales_driven) OVER (PARTITION BY plan) as sales_driven_by_plan,
            SUM(num_sales_driven) OVER () as total_sales_driven,
            SUM(revenue) OVER (PARTITION BY plan, month) as monthly_revenue,
            SUM(revenue) OVER (PARTITION BY plan, quarter) as qtr_revenue,
            SUM(revenue) OVER (PARTITION BY plan) as revenue_by_plan,
            SUM(revenue) OVER () as total_revenue
        FROM groupedby_plans
        ORDER BY 1,2,3
    """
    
    destination = PROCESSED_DIR / target_file_name
    df = client.query(sql).to_dataframe() 
    df.to_csv(destination, index=False, header=True)
    print(f'CSV file created. File name is {target_file_name} and location is {PROCESSED_DIR}')
    

def get_groupby_type(target_file_name: str):
    """
    Extracts counts of various metrics, grouped by charge type, month, and quarter.

    Args:
        target_file_name (str): Input the name you wish the generated CSV to take.
    """
    
    sql = """
        WITH groupedby_types AS (
            SELECT
                type,
                EXTRACT(QUARTER FROM charged_on) as quarter,
                EXTRACT(MONTH FROM charged_on) as month,
                COUNT(*) as num_charges,
                SUM(CASE WHEN is_sales_driven = True THEN 1 ELSE 0 END) as num_sales_driven,
                SUM(amount_usd) as revenue
            FROM `analytics-219613.toggl_take_home_data.monthly_charges_2021`
            GROUP BY 1,2,3 
            ORDER BY 1,2,3
        )

        SELECT
            type,
            month,
            quarter,
            SUM(num_charges) OVER (PARTITION BY type, month) as monthly_count,
            SUM(num_charges) OVER (PARTITION BY type, quarter) as qtr_count,
            SUM(num_charges) OVER (PARTITION BY type) as count_by_type,
            SUM(num_charges) OVER () as total_count,
            SUM(num_sales_driven) OVER (PARTITION BY type, month) as monthly_sales_driven,
            SUM(num_sales_driven) OVER (PARTITION BY type, quarter) as qtr_sales_driven,
            SUM(num_sales_driven) OVER (PARTITION BY type) as sales_driven_by_type,
            SUM(num_sales_driven) OVER () as total_sales_driven,
            SUM(revenue) OVER (PARTITION BY type, month) as monthly_revenue,
            SUM(revenue) OVER (PARTITION BY type, quarter) as qtr_revenue,
            SUM(revenue) OVER (PARTITION BY type) as revenue_by_type,
            SUM(revenue) OVER () as total_revenue
        FROM groupedby_types
        ORDER BY 1,2,3
    """
    
    destination = PROCESSED_DIR / target_file_name
    df = client.query(sql).to_dataframe() 
    df.to_csv(destination, index=False, header=True)
    print(f'CSV file created. File name is {target_file_name} and location is {PROCESSED_DIR}')