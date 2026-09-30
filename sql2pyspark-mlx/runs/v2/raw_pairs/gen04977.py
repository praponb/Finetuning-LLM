from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.order_date").between("2023-05-01", "2024-05-01"))
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("oi.product_id"),
        F.col("oi.unit_price"),
        F.col("o.store_id"),
        F.col("o.amount"),
        F.col("o.status"),
    )
)
