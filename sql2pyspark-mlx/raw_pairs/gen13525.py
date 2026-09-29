from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.filter(F.col("amount").between(1150, 1580)).alias("o"), F.col("oi.order_id") == F.col("o.id"), "left_anti")
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("oi.quantity"),
        F.col("oi.product_id"),
    )
)
