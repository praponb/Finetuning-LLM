from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        "channel",
        F.when(F.col("shipping_fee") >= 41, "high").when(F.col("shipping_fee") >= 20, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
    )
    .filter(F.col("cancelled_count") > 78)
    .orderBy("cancelled_count")
)
