from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .groupBy(
        F.when(F.col("o.channel").isin(["web", "partner"]), "core").otherwise("other").alias("channel_group"),
        F.col("o.store_id"),
    )
    .agg(F.countDistinct(F.col("oi.unit_price")).alias("unique_products"))
    .orderBy("unique_products", "store_id")
)
