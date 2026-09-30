from pyspark.sql import functions as F

base = df_suppliers.filter(F.upper(F.trim("supplier_name")) == "SIAM PARTS CO")

city_summary = (
    base
    .groupBy(
        "city",
        F.when(F.col("rating") >= 4.3, "high").when(F.col("rating") >= 1.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "London", F.col("rating")).otherwise(0)).alias("london_rating"),
    )
)

result = city_summary.filter(F.col("london_rating") >= 71)
