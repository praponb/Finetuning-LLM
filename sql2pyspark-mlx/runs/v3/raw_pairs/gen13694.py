from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.amount") >= 1450)
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("oi.unit_price"),
        F.col("o.status"),
    )
    .orderBy("status")
)
