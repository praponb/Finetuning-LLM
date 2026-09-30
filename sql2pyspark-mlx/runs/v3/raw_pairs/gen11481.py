from pyspark.sql import functions as F

result = (
    df_subscriptions
    .cube(
        F.when(F.col("monthly_fee") >= 236, "high").when(F.col("monthly_fee") >= 82, "mid").otherwise("low").alias("monthly_fee_bucket"),
        "status",
    )
    .agg(F.max("monthly_fee").alias("max_monthly_fee"))
    .orderBy("status")
)
