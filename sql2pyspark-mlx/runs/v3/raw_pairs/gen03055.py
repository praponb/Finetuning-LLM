from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .filter(F.col("oi.quantity") == 6)
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("oi.unit_price"),
        F.col("oi.product_id"),
        F.col("o.amount"),
    )
    .orderBy(F.col("unit_price").desc(), F.col("quantity").desc())
)
