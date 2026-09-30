from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
    )
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("oibp"), F.col("p.id") == F.col("oibp.product_id"), "left")
    .filter(F.col("oibp.revenue") > 73)
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("oibp.revenue"),
        F.col("oibp.unit_price_range"),
    )
    .orderBy("revenue")
)
