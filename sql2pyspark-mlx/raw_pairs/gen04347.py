from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left")
    .filter(F.col("p.brand") == "Stark")
    .select(
        F.col("p.category"),
        F.col("p.launch_date"),
        F.col("oi.quantity"),
        F.col("oi.product_id"),
        F.array_contains(F.col("p.tags"), "new").alias("is_new"),
    )
)
