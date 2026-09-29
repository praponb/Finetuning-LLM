from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .filter((F.col("oi.order_id") == 9) & F.col("o.amount").between(430, 740))
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("o.status"),
        F.col("o.order_date"),
    )
)
