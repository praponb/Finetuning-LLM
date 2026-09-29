from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.filter(F.col("category") == "Electronics").alias("p"), F.col("i.product_id") == F.col("p.id"), "left_semi")
    .filter(F.col("i.store_id").isin([1, 2, 6]))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("i.product_id"),
        F.col("i.last_restock"),
    )
    .orderBy(F.col("last_restock").desc())
)
