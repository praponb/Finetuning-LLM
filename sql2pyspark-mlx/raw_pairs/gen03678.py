from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(F.round(F.avg("balance"), 2).alias("avg_balance"))
    .filter(F.col("avg_balance") >= 44)
    .select("status", "avg_balance")
    .orderBy("status")
    .limit(10)
)
