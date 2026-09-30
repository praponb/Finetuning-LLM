from pyspark.sql import functions as F

result = (
    df_subscriptions
    .rollup(F.date_trunc("month", "start_date").alias("activity_month"))
    .agg(
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_rows"),
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("status") == "CANCELLED", 1)), F.lit(0)), 2).alias("monthly_fee_per_cancelled"),
        F.countDistinct("status").alias("distinct_status"),
    )
    .filter(F.col("monthly_fee_per_cancelled") > 0)
)
