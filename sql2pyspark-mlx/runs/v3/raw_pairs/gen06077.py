from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("status") != "PAUSED") | (F.col("monthly_fee") >= 180))
    .groupBy("status")
    .agg(
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
        F.sum(F.when(F.col("plan") == "standard", F.col("monthly_fee")).otherwise(0)).alias("standard_monthly_fee"),
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
    )
    .orderBy(F.col("monthly_fee_range").desc())
)
