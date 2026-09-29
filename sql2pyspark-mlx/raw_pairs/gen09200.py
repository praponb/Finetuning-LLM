from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .groupBy("product_id")
    .agg(F.count("unit_price").alias("unit_price_count"))
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("oibp"), F.col("p.id") == F.col("oibp.product_id"), "inner")
    .filter(F.col("oibp.unit_price_count") > 4)
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("p.stock_qty"),
        F.col("oibp.unit_price_count"),
    )
    .orderBy("stock_qty")
)
