from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "right")
    .filter(F.col("p.cost") >= 480)
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("p.category"),
        F.col("p.price"),
        F.col("o.customer_id"),
        (F.col("o.discount") - F.col("o.amount")).alias("discount_minus_amount"),
    )
)
