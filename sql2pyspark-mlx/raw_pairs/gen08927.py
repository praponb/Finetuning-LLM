from pyspark.sql import functions as F

filtered_order_items = df_order_items.filter(F.col("order_id") == 39)

order_items_stats = (
    filtered_order_items
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 5, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"))
)

result = (
    order_items_stats
    .select("quantity_bucket", "revenue")
    .orderBy(F.col("revenue").desc(), "quantity_bucket")
    .limit(10)
)
