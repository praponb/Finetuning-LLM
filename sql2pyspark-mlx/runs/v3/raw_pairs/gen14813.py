from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("country").isin(["SG", "DE"]) & (F.col("city") == "Bangkok") & (F.lower(F.trim("supplier_name")) == "siam parts co"))
    .groupBy(
        F.when(F.col("rating") >= 4.4, "high").when(F.col("rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "London", F.col("rating")).otherwise(0)).alias("london_rating"),
    )
    .orderBy(F.col("rating_bucket").desc())
)
