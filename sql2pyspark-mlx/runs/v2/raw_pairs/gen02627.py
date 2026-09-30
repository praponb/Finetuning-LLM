from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("oi.quantity") == 5)
    .groupBy(F.col("o.store_id"))
    .agg(
        F.round(F.avg(F.col("oi.unit_price")), 2).alias("avg_unit_price"),
        F.sum(F.col("oi.unit_price")).alias("total_unit_price"),
    )
    .filter(F.col("avg_unit_price") >= 23)
    .orderBy(F.col("store_id").desc(), F.col("total_unit_price").desc())
)
