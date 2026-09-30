from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("i.store_id").isin([2, 4, 5]) & F.col("i.store_id").isin([3, 4, 5]))
    .select(
        F.col("p.category"),
        F.col("p.cost"),
        F.col("p.brand"),
        F.col("i.product_id"),
        F.col("i.last_restock"),
    )
)
