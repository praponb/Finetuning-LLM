from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([8, 10, 12, 28]) & (F.col("end_date") >= "2023-05-01"))
    .groupBy(
        F.when(F.col("monthly_fee") >= 221, "high").when(F.col("monthly_fee") >= 77, "mid").otherwise("low").alias("monthly_fee_bucket"),
        "plan",
    )
    .agg(
        F.max("end_date").alias("latest_end_date"),
        F.min("start_date").alias("min_start_date"),
    )
)
