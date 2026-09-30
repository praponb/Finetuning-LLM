from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .filter(F.array_contains(F.col("p.tags"), "new") | F.col("oi.order_id").isin([15, 22, 36]))
    .select(
        F.col("p.product_name"),
        F.col("p.launch_date"),
        F.col("oi.unit_price"),
        F.col("oi.product_id"),
    )
)
