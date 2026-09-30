from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.filter(F.col("amount").between(530, 1020)).alias("o"), F.col("oi.order_id") == F.col("o.id"), "left_semi")
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("oi.quantity"),
        F.col("oi.unit_price"),
    )
    .orderBy("id")
)
