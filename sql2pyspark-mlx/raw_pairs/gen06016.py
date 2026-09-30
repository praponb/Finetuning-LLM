from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([9, 12, 18, 20]))
    .groupBy("plan")
    .agg(
        F.count("*").alias("row_count"),
        F.min("monthly_fee").alias("min_monthly_fee"),
        F.count(F.when(F.col("plan") == "premium", 1)).alias("premium_count"),
    )
    .filter(F.col("premium_count") >= 5)
    .drop("premium_count")
    .orderBy(F.col("row_count").desc(), "plan")
    .limit(10)
)
