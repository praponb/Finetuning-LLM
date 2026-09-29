from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(
        F.col("s.country"),
        F.when(F.col("s.rating") >= 3.7, "high").when(F.col("s.rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.count(F.col("p.cost")).alias("cost_count"))
    .filter(F.col("cost_count") > 2)
    .orderBy(F.col("country").desc(), "rating_bucket")
    .limit(5)
)
