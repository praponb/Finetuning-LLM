from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type", "customer_id")
    .pivot("status", ["FROZEN", "CLOSED"])
    .agg(F.sum("balance").alias("total"))
)
