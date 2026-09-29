from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .groupBy("product_id")
    .agg((F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"))
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("oibp"), F.col("p.id") == F.col("oibp.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.stock_qty"),
        F.col("p.supplier_id"),
        F.col("oibp.unit_price_range"),
    )
    .orderBy(F.col("supplier_id").desc())
)
