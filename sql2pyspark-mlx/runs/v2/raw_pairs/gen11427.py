from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "right")
    .filter((F.col("oi.unit_price") <= 360) | F.col("oi.product_id").isin([8, 28, 30, 36]))
    .select(
        F.col("oi.quantity"),
        F.col("oi.unit_price"),
        F.col("oi.product_id"),
        F.col("p.price"),
        F.col("p.brand"),
        F.col("p.category"),
    )
)
