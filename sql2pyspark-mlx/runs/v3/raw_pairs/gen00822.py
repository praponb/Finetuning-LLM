from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([19, 31, 38]))
    .groupBy(
        F.when(F.col("monthly_fee") >= 239, "high").when(F.col("monthly_fee") >= 103, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(((100 * F.sum(F.when(F.col("status") == "EXPIRED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_expired"),
        F.countDistinct("monthly_fee").alias("unique_plans"),
    )
)
