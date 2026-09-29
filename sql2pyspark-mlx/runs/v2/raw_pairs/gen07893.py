from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.filter(F.col("order_date").between("2023-08-17", "2024-07-01")).alias("o"), F.col("oi.order_id") == F.col("o.id"), "left_semi")
    .filter(F.col("oi.product_id") == 22)
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("oi.quantity"),
        F.col("oi.order_id"),
    )
)
