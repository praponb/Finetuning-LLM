from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .filter(F.col("o.store_id") == 21)
    .groupBy(
        F.when(F.col("o.status").isin(["SHIPPED", "RETURNED"]), "tier_1").otherwise("other").alias("status_group"),
        F.col("o.channel"),
    )
    .agg(
        F.count(F.col("oi.unit_price")).alias("unit_price_count"),
        F.count(F.col("oi.quantity")).alias("quantity_count"),
    )
    .orderBy(F.col("unit_price_count").desc())
)
