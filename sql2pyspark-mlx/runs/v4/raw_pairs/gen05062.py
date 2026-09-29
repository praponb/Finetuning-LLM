from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(
        F.count(F.when(F.col("plan") == "premium", 1)).alias("premium_count"),
        F.count(F.when(F.col("plan") == "standard", 1)).alias("standard_count"),
        F.stddev_samp("monthly_fee").alias("monthly_fee_stddev"),
    )
    .orderBy(F.col("plan").desc_nulls_last())
)
