from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") <= 3.5) | (F.upper(F.trim("supplier_name")) == "SIAM PARTS CO"))
    .groupBy(
        F.when(F.col("rating") >= 3.5, "high").when(F.col("rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.round(F.avg("rating"), 2).alias("avg_rating"))
    .filter(F.col("avg_rating") > 42)
)
