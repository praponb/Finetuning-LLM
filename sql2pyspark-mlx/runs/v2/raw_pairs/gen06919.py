from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("oi.quantity") < 8)
    .select(
        F.col("oi.unit_price"),
        F.col("oi.quantity"),
        F.col("o.order_date"),
        F.col("o.channel"),
    )
)
