from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 226, "high").when(F.col("monthly_fee") >= 98, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(F.countDistinct("monthly_fee").alias("unique_statuss"))
    .orderBy(F.col("unique_statuss").desc())
)
