from pyspark.sql import functions as F

recent_accounts = (
    df_accounts
    .filter(F.col("opened_date").between("2023-07-01", "2024-11-01"))
    .withColumn("opened_date_month_end", F.last_day("opened_date"))
)

status_summary = (
    recent_accounts
    .groupBy("status")
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.countDistinct("balance").alias("unique_statuss"),
    )
)

result = (
    status_summary
    .filter(F.col("unique_statuss") < 8)
    .select("status", "avg_balance")
    .orderBy("status", "avg_balance")
)
