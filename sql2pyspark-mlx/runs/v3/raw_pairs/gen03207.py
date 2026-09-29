from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.channel") != "web")
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("oi.product_id"),
        F.col("o.store_id"),
        F.col("o.customer_id"),
        F.col("o.amount"),
    )
)
