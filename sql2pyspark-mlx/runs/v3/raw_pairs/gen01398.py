from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNull())
    .groupBy(
        F.when(F.col("discount") >= 0.44, "high").when(F.col("discount") >= 0.17, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(
        F.avg("shipping_fee").alias("avg_shipping_fee"),
        F.sum(F.when(F.col("status") == "SHIPPED", F.col("discount")).otherwise(0)).alias("shipped_discount"),
    )
    .filter(F.col("shipped_discount") < 41)
    .select("discount_bucket", "avg_shipping_fee", "shipped_discount")
    .orderBy("shipped_discount")
)
