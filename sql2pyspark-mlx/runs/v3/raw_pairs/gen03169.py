from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "outer")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .filter(F.col("o.discount") > 0.24)
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("oi.order_id"),
        F.col("p.product_name"),
        F.col("o.amount"),
    )
)
