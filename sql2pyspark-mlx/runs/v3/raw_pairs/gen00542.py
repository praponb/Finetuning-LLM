from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["TH", "SG"]))
    .groupBy(
        F.when(F.col("rating") >= 3.8, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0)).alias("tokyo_count"))
    .filter(F.col("tokyo_count") > 29)
    .orderBy("city")
)
