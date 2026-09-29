from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .groupBy(F.col("o.channel"), F.col("o.status"))
    .agg(
        (F.max(F.col("oi.unit_price")) - F.min(F.col("oi.unit_price"))).alias("unit_price_range"),
        F.avg(F.col("oi.quantity")).alias("avg_quantity"),
    )
    .orderBy(F.col("avg_quantity").desc(), F.col("unit_price_range").desc())
)
