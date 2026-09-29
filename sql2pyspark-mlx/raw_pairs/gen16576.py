from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("oi.product_id"),
        F.col("p.category"),
        F.col("o.customer_id"),
    )
    .orderBy(F.col("id").desc(), F.col("product_id").desc())
    .limit(50)
)
