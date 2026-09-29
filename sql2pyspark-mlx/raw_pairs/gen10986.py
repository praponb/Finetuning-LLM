from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("o.customer_id"),
        F.col("o.amount"),
        F.col("p.brand"),
        F.round(F.col("o.amount"), 1).alias("amount_rounded"),
    )
)
