from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        "channel",
        F.when(F.col("discount") >= 0.32, "high").when(F.col("discount") >= 0.17, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(
        F.max("amount").alias("max_amount"),
        F.avg("discount").alias("avg_discount"),
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_count"),
    )
)
