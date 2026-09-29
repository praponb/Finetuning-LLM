from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & (F.col("status") != "ACTIVE"))
    .groupBy("status")
    .agg(
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.max("monthly_fee").alias("max_monthly_fee"),
    )
)
