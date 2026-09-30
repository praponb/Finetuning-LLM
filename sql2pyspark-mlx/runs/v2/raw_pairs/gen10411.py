from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.opened_date") >= "2024-07-01")
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("o.order_date"),
        F.col("o.store_id"),
        F.col("s.sqft"),
    )
)
