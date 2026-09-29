from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(
        F.col("s.city"),
        F.when(F.col("s.rating") >= 3.8, "high").when(F.col("s.rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.count(F.when(F.col("p.category") == "Sports", 1)).alias("sports_count"),
        F.min(F.col("p.launch_date")).alias("first_launch_date"),
    )
)
