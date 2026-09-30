from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("i.product_id").isin([4, 6]))
    .groupBy(
        F.col("s.city"),
        F.when(F.col("s.rating") >= 4.3, "high").when(F.col("s.rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.min(F.col("i.last_restock")).alias("first_last_restock"),
        F.max(F.col("i.last_restock")).alias("last_last_restock"),
    )
)
