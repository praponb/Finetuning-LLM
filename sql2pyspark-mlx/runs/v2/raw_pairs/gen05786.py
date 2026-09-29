from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.brand").isin(["Stark", "Globex"]) & F.col("i.product_id").isin([4, 6]))
    .select(
        F.col("p.cost"),
        F.col("p.supplier_id"),
        F.col("p.category"),
        F.col("i.last_restock"),
        F.col("i.quantity"),
        F.col("i.store_id"),
    )
    .orderBy("supplier_id", F.col("store_id").desc())
)
