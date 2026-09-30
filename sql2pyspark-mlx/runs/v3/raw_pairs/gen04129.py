from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("plan") == "basic") & (F.col("start_date") < "2023-12-01"))
    .groupBy("plan")
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.count(F.when(F.col("plan") == "standard", 1)).alias("standard_count"),
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
    )
    .filter(F.col("total_monthly_fee") >= 1190)
    .drop("total_monthly_fee")
    .orderBy(F.col("stddev_monthly_fee").desc())
)
