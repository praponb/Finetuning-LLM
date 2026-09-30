from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.quarter("opened_date").alias("opened_quarter"), "status")
    .agg(F.avg("balance").alias("avg_balance"))
    .filter(F.col("avg_balance") > 58000)
    .select("opened_quarter", "status", "avg_balance")
)
