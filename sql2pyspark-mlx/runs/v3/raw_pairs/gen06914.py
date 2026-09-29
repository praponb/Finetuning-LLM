from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("products.id") == F.col("order_items.product_id"), "inner")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "outer")
    .groupBy(
        F.when(F.col("orders.channel").isin(["web", "store"]), "group_a").otherwise("other").alias("channel_group"),
    )
    .agg(F.count(F.col("order_items.unit_price")).alias("unit_price_count"))
    .orderBy(F.col("unit_price_count").desc())
)
