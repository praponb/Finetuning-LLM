from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id") == 15)
    .select("id", "customer_id", "opened_date")
)
