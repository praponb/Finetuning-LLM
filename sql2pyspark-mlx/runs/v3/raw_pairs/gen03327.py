from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.discount").between(0.31, 0.39))
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("oi.unit_price"),
        F.col("o.amount"),
        F.col("o.order_date"),
        F.col("o.customer_id"),
    )
    .orderBy(F.col("customer_id").desc())
)
