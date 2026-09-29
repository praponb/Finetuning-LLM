from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
        F.count(F.when(F.col("plan") == "premium", 1)).alias("premium_count"),
        F.count("*").alias("subscription_count"),
    )
    .filter(F.col("subscription_count") >= 5)
    .orderBy(F.col("monthly_fee_range").desc())
)
