from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("i.store_id").isin([1, 4, 6]))
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("s.store_name"),
        F.col("p.brand"),
    )
    .orderBy(F.col("store_name").desc(), "id")
    .limit(5)
)
