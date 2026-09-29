from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type", "customer_id")
    .pivot("status", ["OPEN", "FROZEN", "CLOSED"])
    .agg(F.max("balance").alias("amount"), F.count(F.lit(1)).alias("n"))
)
