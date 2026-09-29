from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .groupBy(
        F.when(F.col("s.rating") >= 4.3, "high").when(F.col("s.rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
        F.col("s.country"),
    )
    .agg(F.countDistinct(F.col("p.price")).alias("unique_brands"))
    .filter(F.col("unique_brands") > 3)
)
