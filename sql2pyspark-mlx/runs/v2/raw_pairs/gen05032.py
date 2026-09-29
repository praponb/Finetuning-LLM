from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .filter(F.col("o.channel").isNotNull())
    .groupBy(F.col("p.brand"))
    .agg(
        F.sum(F.col("oi.quantity")).alias("total_quantity"),
        F.round(F.avg(F.col("oi.quantity")), 2).alias("avg_quantity"),
    )
)
