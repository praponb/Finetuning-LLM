from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "inner")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .filter(F.size(F.col("products.tags")) >= 1)
    .groupBy(F.col("orders.channel"))
    .agg(
        F.count("*").alias("order_item_count"),
        F.sum(F.col("order_items.unit_price")).alias("total_unit_price"),
    )
    .filter(F.col("total_unit_price") >= 2400)
    .orderBy(F.col("channel").desc())
)
