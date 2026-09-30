from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "right")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter((F.col("o.shipping_fee") >= 31) & (F.col("o.status") != "COMPLETED"))
    .select(
        F.col("oi.quantity"),
        F.col("oi.order_id"),
        F.col("o.amount"),
        F.col("s.store_name"),
    )
    .orderBy(F.col("quantity").desc(), F.col("order_id").desc())
)
