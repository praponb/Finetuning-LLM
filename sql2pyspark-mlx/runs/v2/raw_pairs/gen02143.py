from pyspark.sql import functions as F

filtered_suppliers = (
    df_suppliers
    .filter(F.col("country").isin(["DE", "SG", "JP"]) & (F.col("country") == "SG") & (F.col("country") != "US"))
)

summary = (
    filtered_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.8, "high").when(F.col("rating") >= 2.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        (F.max("rating") - F.min("rating")).alias("rating_range"),
        F.sum(F.when(F.col("country") == "DE", 1).otherwise(0)).alias("de_count"),
    )
)

result = (
    summary
    .orderBy("rating_bucket")
    .limit(5)
)
