from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(
        F.when(F.col("s.rating") >= 4.3, "high").when(F.col("s.rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
        F.col("s.city"),
    )
    .agg(F.min(F.col("p.launch_date")).alias("first_launch_date"))
    .orderBy("rating_bucket", "city")
    .limit(20)
)
