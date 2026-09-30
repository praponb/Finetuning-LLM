from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "right")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "left")
    .select(
        F.col("oi.quantity"),
        F.col("oi.unit_price"),
        F.col("o.status"),
        F.col("s.city"),
    )
    .orderBy(F.col("quantity").desc())
)
