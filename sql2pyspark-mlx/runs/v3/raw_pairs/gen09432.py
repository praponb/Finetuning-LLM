from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.length(F.col("s.store_name")) >= 6)
    .groupBy(F.date_trunc("MONTH", F.col("o.order_date")).alias("order_month"))
    .agg(
        F.avg(F.col("oi.unit_price")).alias("avg_unit_price"),
        F.min(F.col("oi.unit_price")).alias("min_unit_price"),
    )
)
