from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(
        F.col("s.city"),
        F.when(F.col("s.rating") >= 3.7, "high").when(F.col("s.rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.count("*").alias("num_products"),
        F.sum(F.when(F.col("p.brand") == "Stark", 1).otherwise(0)).alias("stark_count"),
    )
)
