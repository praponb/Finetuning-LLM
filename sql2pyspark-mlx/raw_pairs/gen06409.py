from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.city") == "Berlin")
    .groupBy(
        F.when(F.col("s.rating") >= 3.9, "high").when(F.col("s.rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.max(F.col("p.launch_date")).alias("last_launch_date"))
    .orderBy("last_launch_date")
)
