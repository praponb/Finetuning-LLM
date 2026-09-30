from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("o.id") == F.col("oi.order_id"), "left")
    .filter(F.col("o.channel").isin(["web", "store"]))
    .select(
        F.col("o.channel"),
        F.col("oi.order_id"),
        F.col("oi.unit_price"),
        F.col("oi.quantity"),
    )
    .orderBy(F.col("quantity").desc())
)
