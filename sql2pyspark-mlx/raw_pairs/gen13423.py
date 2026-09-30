from pyspark.sql import functions as F

result = (
    df_order_items
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.quantity",
        "e.unit_price",
        F.col("b.amount"),
        F.col("b.discount"),
        F.col("b.status"),
    )
)
