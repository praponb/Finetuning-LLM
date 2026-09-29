from pyspark.sql import functions as F

result = (
    df_customers
    .select("signup_date")
    .subtract(df_accounts.select("opened_date"))
)
