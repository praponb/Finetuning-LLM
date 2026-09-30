from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.month("end_date").alias("end_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cancelled"),
        F.min("start_date").alias("min_start_date"),
    )
    .filter(F.col("pct_cancelled") >= 63)
    .select("end_month", "pct_cancelled", "min_start_date")
    .orderBy(F.col("min_start_date").desc(), F.col("end_month").desc())
)
