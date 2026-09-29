from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("oi.order_id").isin([5, 18]))
    .select(
        F.col("o.store_id"),
        F.col("o.order_date"),
        F.col("o.amount"),
        F.col("oi.quantity"),
        F.col("oi.product_id"),
        F.col("oi.unit_price"),
    )
    .orderBy("amount")
)
