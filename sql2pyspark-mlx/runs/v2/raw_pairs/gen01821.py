from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.sqft") < 8313)
    .groupBy(F.col("o.status"))
    .agg(
        (F.max(F.col("oi.quantity")) - F.min(F.col("oi.quantity"))).alias("quantity_range"),
    )
)
