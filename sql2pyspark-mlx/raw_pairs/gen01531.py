from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.count("balance").alias("balance_count"),
        F.avg("balance").alias("avg_balance"),
    )
    .filter(F.col("balance_count") >= 10)
    .select("status", "balance_count", "avg_balance")
    .orderBy(F.col("avg_balance").desc())
)
