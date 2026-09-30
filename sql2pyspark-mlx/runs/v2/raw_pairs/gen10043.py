from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .groupBy(F.col("o.channel"))
    .agg(
        F.min(F.col("oi.quantity")).alias("min_quantity"),
        F.round(F.avg(F.col("oi.unit_price")), 2).alias("avg_unit_price"),
    )
    .filter(F.col("avg_unit_price") >= 59)
    .orderBy(F.col("avg_unit_price").desc())
)
