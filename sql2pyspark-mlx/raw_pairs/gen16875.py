from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(
        F.when(F.col("s.rating") >= 4.2, "high").when(F.col("s.rating") >= 1.5, "mid").otherwise("low").alias("rating_bucket"),
        F.col("s.city"),
    )
    .agg(F.max(F.col("i.last_restock")).alias("last_last_restock"))
    .orderBy("rating_bucket", "city")
    .limit(5)
)
