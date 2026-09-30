from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("p.supplier_id") == 31)
    .groupBy(
        F.when(F.col("s.rating") >= 4.5, "high").when(F.col("s.rating") >= 1.4, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.count("*").alias("num_products"),
        F.count("*").alias("product_count"),
    )
    .orderBy(F.col("num_products").desc())
)
