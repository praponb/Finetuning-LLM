from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.array_contains(F.col("p.tags"), "clearance"))
    .groupBy(
        F.when(F.col("s.rating") >= 4.5, "high").when(F.col("s.rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.count(F.when(F.col("p.category") == "Home", 1)).alias("home_count"))
    .orderBy("rating_bucket", F.col("home_count").desc())
)
