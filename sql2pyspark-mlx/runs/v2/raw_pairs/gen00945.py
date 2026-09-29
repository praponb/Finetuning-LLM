from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.city"), F.month(F.col("s.opened_date")).alias("opened_month"))
    .agg(F.count("*").alias("num_order_items"))
    .orderBy(F.col("num_order_items").desc(), F.col("opened_month").desc(), "city")
    .limit(5)
)
