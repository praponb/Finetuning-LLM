from pyspark.sql import functions as F

src = (
    df_subscriptions
    .filter(F.month("end_date").isin([1, 3, 8]) & (F.col("status") == "EXPIRED") & (F.col("monthly_fee") <= 130))
)

summary = (
    src
    .groupBy("status")
    .agg(
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_count"),
    )
)

result = (
    summary
    .filter(F.col("max_monthly_fee") >= 120)
    .select("status", "max_monthly_fee", "cancelled_count")
)
