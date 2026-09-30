from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .groupBy(
        F.col("s.city"),
        F.when(F.col("s.rating") >= 3.9, "high").when(F.col("s.rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.min(F.col("p.launch_date")).alias("min_launch_date"))
    .orderBy(F.col("city").desc())
)
