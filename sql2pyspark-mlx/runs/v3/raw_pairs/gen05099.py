from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "right")
    .filter(F.col("oi.unit_price") <= 280)
    .select(
        F.col("oi.quantity"),
        F.col("oi.order_id"),
        F.col("p.launch_date"),
        F.col("p.category"),
        F.col("p.brand"),
        F.array_contains(F.col("p.tags"), "premium").alias("is_premium"),
    )
)
