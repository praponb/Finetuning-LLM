from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.city") != "Chiang Mai")
    .groupBy(
        F.when(F.col("s.rating") >= 3.5, "high").when(F.col("s.rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.countDistinct(F.col("p.price")).alias("unique_suppliers"))
    .filter(F.col("unique_suppliers") > 4)
    .orderBy("rating_bucket")
    .limit(10)
)
