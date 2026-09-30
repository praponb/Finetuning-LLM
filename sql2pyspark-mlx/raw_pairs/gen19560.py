from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(
        F.when(F.col("s.rating") >= 3.5, "high").when(F.col("s.rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"),
        F.col("s.city"),
    )
    .agg(F.count(F.col("p.price")).alias("price_count"))
    .orderBy("rating_bucket", F.col("price_count").desc(), "city")
    .limit(20)
)
