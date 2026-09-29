from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "left")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .select(
        F.col("oi.quantity"),
        F.col("oi.product_id"),
        F.col("p.cost"),
        F.col("o.customer_id"),
    )
    .orderBy("customer_id")
)
