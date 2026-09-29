from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(130, 180) & (F.col("customer_id") == 32))
    .groupBy("status")
    .agg(
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.sum(F.when(F.col("plan") == "basic", 1).otherwise(0)).alias("basic_count"),
    )
)
