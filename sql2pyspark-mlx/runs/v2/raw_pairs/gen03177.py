from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("oi.unit_price") <= 110)
    .groupBy(F.col("o.status"))
    .agg(
        F.min(F.col("oi.quantity")).alias("min_quantity"),
        F.round(F.avg(F.col("oi.unit_price")), 2).alias("avg_unit_price"),
    )
    .filter(F.col("avg_unit_price") > 46)
)
