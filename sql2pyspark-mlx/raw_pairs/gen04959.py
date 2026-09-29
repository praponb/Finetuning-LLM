from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .groupBy(
        F.col("o.status"),
        F.date_trunc("MONTH", F.col("o.order_date")).alias("order_month"),
    )
    .agg(
        F.count(F.col("oi.unit_price")).alias("unit_price_count"),
        F.count("*").alias("order_item_count"),
    )
)
