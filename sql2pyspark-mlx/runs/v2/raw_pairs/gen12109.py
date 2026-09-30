from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("i.store_id").isin([4, 5]))
    .select(
        F.col("i.quantity"),
        F.col("i.store_id"),
        F.col("s.store_name"),
        F.col("s.city"),
        F.col("p.cost"),
        F.col("p.category"),
    )
    .orderBy("store_id", F.col("quantity").desc())
)
