from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(((F.col("opened_date") >= "2023-11-01") & F.col("customer_id").isin([1, 31])) | (F.col("opened_date") > "2025-10-01"))
    .groupBy("status", F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.max("opened_date").alias("latest_opened_date"),
    )
    .filter(F.col("avg_balance") >= 46000)
    .orderBy("opened_month")
)
