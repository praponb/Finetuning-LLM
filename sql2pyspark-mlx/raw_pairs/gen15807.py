from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.country") == "UK")
    .groupBy(
        F.when(F.col("s.rating") >= 3.6, "high").when(F.col("s.rating") >= 1.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.max(F.col("p.cost")).alias("max_cost"))
    .filter(F.col("max_cost") >= 310)
    .orderBy("rating_bucket")
)
