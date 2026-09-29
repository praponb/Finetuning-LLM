from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("p.launch_date") < "2025-07-01")
    .groupBy(
        F.when(F.col("s.rating") >= 3.7, "high").when(F.col("s.rating") >= 1.4, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("p.category") == "Home", 1)).alias("home_count"),
    )
)
