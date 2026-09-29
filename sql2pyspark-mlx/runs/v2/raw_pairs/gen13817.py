from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "outer")
    .filter(F.col("i.product_id").isin([1, 7, 8, 9]))
    .select(
        F.col("i.quantity"),
        F.col("i.product_id"),
        F.col("s.city"),
        F.col("p.price"),
        F.col("p.product_name"),
    )
    .orderBy(F.col("product_name").desc())
)
