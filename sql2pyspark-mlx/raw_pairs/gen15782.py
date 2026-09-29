from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("oi.unit_price"),
        F.col("oi.quantity"),
        F.col("o.amount"),
        F.col("s.opened_date"),
        F.initcap(F.trim(F.col("o.channel"))).alias("channel_title"),
    )
    .orderBy("opened_date")
)
