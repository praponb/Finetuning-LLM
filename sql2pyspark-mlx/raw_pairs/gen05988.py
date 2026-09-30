from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("status") == "FROZEN") & F.col("customer_id").isin([10, 24]))
    .select("id", "account_type", "customer_id", "status")
)
