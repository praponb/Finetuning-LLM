from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.product_name").like("U%") & F.col("i.product_id").isin([2, 6, 9]))
    .select(
        F.col("i.product_id"),
        F.col("i.store_id"),
        F.col("i.last_restock"),
        F.col("p.category"),
        F.col("p.launch_date"),
        F.col("p.brand"),
    )
)
