from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("country").isin(["US", "JP", "UK"]))
    .withColumn("city_supplier_name_key", F.concat_ws("-", "city", "supplier_name"))
)

summary = (
    base
    .groupBy(
        F.when(F.col("rating") >= 4.0, "high").when(F.col("rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "Singapore", F.col("rating")).otherwise(0)).alias("singapore_rating"),
    )
)

result = (
    summary
    .filter(F.col("singapore_rating") > 65)
    .select("rating_bucket", "singapore_rating")
    .orderBy(F.col("singapore_rating").desc())
)
