from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("i.store_id").isin([2, 3, 6]))
    .select(
        F.col("i.last_restock"),
        F.col("i.store_id"),
        F.col("p.price"),
        F.col("p.category"),
    )
)
