from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.rating") < 3.5)
    .groupBy(
        F.when(F.col("s.rating") >= 3.5, "high").when(F.col("s.rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.round(F.avg(F.col("p.cost")), 2).alias("avg_cost"))
    .orderBy("avg_cost", "rating_bucket")
    .limit(3)
)
