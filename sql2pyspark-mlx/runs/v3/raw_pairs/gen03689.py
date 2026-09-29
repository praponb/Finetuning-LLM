from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
        F.avg("quantity").alias("avg_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.revenue") >= 27)
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("t.revenue"),
        F.col("t.avg_quantity"),
    )
)
