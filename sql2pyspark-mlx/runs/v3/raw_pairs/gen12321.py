from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .filter(F.col("oi.quantity").between(6, 7))
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("o.customer_id"),
        F.col("o.status"),
        F.round(F.col("oi.quantity"), 1).alias("quantity_rounded"),
    )
    .orderBy(F.col("status").desc(), "id")
)
