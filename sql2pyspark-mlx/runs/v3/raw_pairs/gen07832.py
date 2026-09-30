from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 247, "high").when(F.col("monthly_fee") >= 121, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(F.min("end_date").alias("first_end_date"))
    .orderBy("first_end_date")
)
