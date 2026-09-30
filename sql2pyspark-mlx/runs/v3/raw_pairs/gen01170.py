from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("oi.unit_price") >= 300)
    .select(
        F.col("oi.quantity"),
        F.col("oi.order_id"),
        F.col("oi.unit_price"),
        F.col("o.status"),
        F.col("o.store_id"),
        F.col("o.customer_id"),
    )
    .orderBy(F.col("unit_price").desc())
)
