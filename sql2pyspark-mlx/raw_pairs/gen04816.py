from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 260, "high").when(F.col("monthly_fee") >= 111, "mid").otherwise("low").alias("monthly_fee_bucket"),
        "status",
    )
    .agg((F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"))
    .filter(F.col("monthly_fee_range") < 42)
    .orderBy(F.col("status").desc())
)
