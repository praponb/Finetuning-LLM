from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.count("unit_price").alias("unit_price_count"),
        F.sum("quantity").alias("total_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .select(
        F.col("p.cost"),
        F.col("p.stock_qty"),
        F.col("t.unit_price_count"),
        F.col("t.total_quantity"),
    )
    .orderBy("stock_qty", F.col("total_quantity").desc())
)
