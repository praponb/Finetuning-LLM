from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .select(
        F.col("oi.quantity"),
        F.col("oi.product_id"),
        F.col("p.price"),
        F.col("o.status"),
        F.array_join(F.col("p.tags"), ", ").alias("tags_text"),
    )
    .orderBy(F.col("quantity").desc(), F.col("product_id").desc())
)
