from pyspark.sql import functions as F

filtered_accounts = df_accounts.filter(F.col("balance").between(24000, 32000))

status_summary = (
    filtered_accounts
    .groupBy("status")
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.count("*").alias("row_count"),
    )
)

result = (
    status_summary
    .filter(F.col("row_count") > 6)
    .select("status", "row_count")
    .orderBy("row_count")
)
