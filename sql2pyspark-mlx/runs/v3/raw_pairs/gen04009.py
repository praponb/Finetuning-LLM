from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") >= "2025-05-11")
    .groupBy(F.month("end_date").alias("end_month"))
    .agg(
        F.countDistinct("monthly_fee").alias("unique_statuss"),
        F.count(F.when(F.col("plan") == "premium", 1)).alias("premium_count"),
    )
    .filter(F.col("premium_count") >= 6)
    .select("end_month", "unique_statuss", "premium_count")
    .orderBy(F.col("premium_count").desc())
)
