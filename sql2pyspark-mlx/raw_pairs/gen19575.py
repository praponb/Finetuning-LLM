from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("oi.product_id").isin([1, 14]))
    .select(
        F.col("o.channel"),
        F.col("o.discount"),
        F.col("o.status"),
        F.col("oi.product_id"),
        F.col("oi.quantity"),
    )
)
