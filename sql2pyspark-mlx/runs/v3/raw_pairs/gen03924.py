from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("i.store_id").isin([1, 2, 6]))
    .select(
        F.col("p.brand"),
        F.col("i.product_id"),
        F.col("i.store_id"),
        F.col("i.last_restock"),
    )
    .orderBy("product_id")
)
