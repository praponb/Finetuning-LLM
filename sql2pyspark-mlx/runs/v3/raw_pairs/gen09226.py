from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id", "status")
    .agg(F.sum("balance").alias("total_balance"))
    .filter(F.col("total_balance") > 363000)
    .select("customer_id", "status", "total_balance")
)
