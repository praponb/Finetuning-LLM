from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("customer_id") == 12) & F.col("start_date").between("2023-04-04", "2025-03-01"))
    .select(
        "id",
        "status",
        "start_date",
        F.ceil("monthly_fee").alias("monthly_fee_whole"),
    )
    .orderBy("start_date", "id")
    .limit(3)
)
