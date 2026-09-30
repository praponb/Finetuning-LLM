from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .filter(F.col("p.brand").isNotNull())
    .groupBy(
        F.when(F.col("o.channel").isin(["mobile", "partner"]), "tier_1").otherwise("other").alias("channel_group"),
    )
    .agg(
        F.max(F.col("oi.unit_price")).alias("max_unit_price"),
        F.round(F.avg(F.col("oi.unit_price")), 2).alias("avg_unit_price"),
    )
    .filter(F.col("max_unit_price") > 130)
    .orderBy("avg_unit_price")
)
