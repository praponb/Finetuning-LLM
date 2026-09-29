from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .agg(
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.sum(F.when(F.col("plan") == "standard", 1).otherwise(0)).alias("standard_count"),
    )
    .filter(F.col("standard_count") > 54)
    .drop("standard_count")
)
