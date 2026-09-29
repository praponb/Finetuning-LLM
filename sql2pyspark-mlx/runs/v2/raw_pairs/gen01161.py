from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.last_restock"),
        F.col("p.product_name"),
        F.col("p.launch_date"),
        F.col("s.opened_date"),
        F.array_contains(F.col("p.tags"), "eco").alias("is_eco"),
    )
)
