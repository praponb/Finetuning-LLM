from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .groupBy(
        F.when(F.col("s.rating") >= 3.6, "high").when(F.col("s.rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.avg(F.col("p.cost")).alias("avg_cost"))
)
