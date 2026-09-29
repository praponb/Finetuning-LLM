from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") != "UK")
    .groupBy("country", "city")
    .agg(
        F.min("rating").alias("min_rating"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"),
    )
    .orderBy(F.col("country").desc())
)
