from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(
        F.when(F.col("s.rating") >= 3.5, "high").when(F.col("s.rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"),
        F.col("s.country"),
    )
    .agg(
        F.countDistinct(F.col("i.quantity")).alias("unique_products"),
        F.min(F.col("i.last_restock")).alias("first_last_restock"),
    )
)
