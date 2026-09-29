from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left_semi")
    .filter(F.col("p.supplier_id").isin([7, 8, 34, 35]))
    .select(
        F.col("p.id"),
        F.col("p.category"),
        F.col("p.launch_date"),
        F.col("p.product_name"),
    )
)
