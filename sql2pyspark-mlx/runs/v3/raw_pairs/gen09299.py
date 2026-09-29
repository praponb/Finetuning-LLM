from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "left")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("oi.quantity"),
        F.col("p.category"),
        F.col("o.discount"),
        F.col("o.customer_id"),
    )
    .orderBy(F.col("unit_price").desc())
)
