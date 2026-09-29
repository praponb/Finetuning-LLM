from pyspark.sql import functions as F

recent_subscriptions = (
    df_subscriptions
    .filter((F.col("plan") == "premium") & (F.year("start_date") == 2024))
)

agg = (
    recent_subscriptions
    .groupBy("status")
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.max("end_date").alias("last_end_date"),
    )
)

result = (
    agg
    .filter(F.col("monthly_fee_count") >= 5)
    .orderBy("last_end_date", F.col("status").desc())
)
