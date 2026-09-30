from pyspark.sql import functions as F

recent_order_items = df_order_items.filter(F.col("product_id").isNull())

agg = (
    recent_order_items
    .groupBy("product_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.countDistinct("quantity").alias("unique_products"),
    )
)

result = (
    agg
    .select("product_id", "avg_quantity", "unique_products")
    .orderBy("avg_quantity", F.col("unique_products").desc())
)
