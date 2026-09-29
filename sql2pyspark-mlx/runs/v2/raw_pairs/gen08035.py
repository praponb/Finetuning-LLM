from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.rating").between(2.0, 3.1))
    .groupBy(
        F.when(F.col("s.rating") >= 3.5, "high").when(F.col("s.rating") >= 2.3, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.count("*").alias("total_count"),
        F.sum(F.col("p.price")).alias("total_price"),
    )
    .orderBy(F.col("total_count").desc(), "total_price")
)
