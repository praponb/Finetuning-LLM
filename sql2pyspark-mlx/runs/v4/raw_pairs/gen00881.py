from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([17, 34, 37]))
    .groupBy(
        F.date_format("opened_date", "yyyy-MM").alias("opened_month"),
        "account_type",
    )
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_count"),
    )
    .filter(F.col("frozen_count") >= 2)
    .drop("frozen_count")
)
