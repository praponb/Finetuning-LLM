from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("o.shipping_fee") <= 28)
    .groupBy(F.col("s.region"))
    .agg(
        F.avg(F.col("oi.quantity")).alias("avg_quantity"),
        (F.max(F.col("oi.quantity")) - F.min(F.col("oi.quantity"))).alias("quantity_range"),
    )
    .orderBy("region")
    .limit(3)
)
