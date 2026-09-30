from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_order_items.alias("order_items"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .filter(F.col("order_items.unit_price") > 320)
    .groupBy(F.col("products.id"), F.col("products.product_name"))
    .agg(
        F.count(F.col("order_items.id")).alias("order_item_count"),
        (F.max(F.col("order_items.unit_price")) - F.min(F.col("order_items.unit_price"))).alias("unit_price_range"),
    )
    .orderBy("product_name", F.col("unit_price_range").desc())
)
