from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([6, 21, 32]) & (F.col("opened_date") >= "2024-03-01"))
    .select("status", "customer_id")
)
