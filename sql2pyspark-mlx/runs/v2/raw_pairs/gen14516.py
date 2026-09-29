from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("orders.shipping_fee") >= 39)
    .groupBy(
        F.when(F.col("stores.sqft") >= 17067, "high").when(F.col("stores.sqft") >= 3202, "mid").otherwise("low").alias("sqft_bucket"),
        F.col("stores.region"),
    )
    .agg(
        F.min(F.col("orders.order_date")).alias("min_order_date"),
        F.sum(F.when(F.col("orders.status") == "SHIPPED", F.col("orders.shipping_fee")).otherwise(0)).alias("shipped_shipping_fee"),
    )
    .filter(F.col("shipped_shipping_fee") > 75)
    .orderBy(F.col("shipped_shipping_fee").desc(), "region", "sqft_bucket")
    .limit(5)
)
