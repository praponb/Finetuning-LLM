from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .groupBy(
        F.when(F.col("s.rating") >= 3.7, "high").when(F.col("s.rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.min(F.col("i.last_restock")).alias("first_last_restock"))
    .orderBy("rating_bucket")
)
