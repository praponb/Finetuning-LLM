from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.count("*").alias("order_item_count"),
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
    )
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("oibp"), F.col("p.id") == F.col("oibp.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.supplier_id"),
        F.col("p.launch_date"),
        F.col("oibp.order_item_count"),
        F.col("oibp.revenue"),
    )
)
