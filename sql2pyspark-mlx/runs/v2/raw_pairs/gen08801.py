from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("i.store_id").isin([1, 3]))
    .groupBy(
        F.when(F.col("s.rating") >= 3.7, "high").when(F.col("s.rating") >= 1.8, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.countDistinct(F.col("i.quantity")).alias("unique_stores"),
        F.min(F.col("i.last_restock")).alias("min_last_restock"),
    )
    .orderBy(F.col("rating_bucket").desc())
)
