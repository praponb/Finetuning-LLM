from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.opened_date") <= "2024-11-01")
    .groupBy(F.col("o.status"))
    .agg(F.sum(F.col("oi.unit_price")).alias("total_unit_price"))
    .filter(F.col("total_unit_price") > 2400)
)
