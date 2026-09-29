from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type", "status")
    .agg(F.round(F.avg("balance"), 2).alias("avg_balance"))
    .filter(F.col("avg_balance") >= 74)
    .orderBy(F.col("account_type").desc())
)
