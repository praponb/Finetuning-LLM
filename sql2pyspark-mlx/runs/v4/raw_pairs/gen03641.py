from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 207, "high").when(F.col("monthly_fee") >= 56, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
    )
    .orderBy(F.col("monthly_fee_range").desc(), "monthly_fee_bucket")
    .limit(50)
)
