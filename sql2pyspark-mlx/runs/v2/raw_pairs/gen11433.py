from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") <= 1.8)
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("city") == "Tokyo", F.col("rating")).otherwise(0)).alias("tokyo_rating"),
        F.min("rating").alias("min_rating"),
    )
    .filter(F.col("min_rating") > 3.5)
    .drop("min_rating")
)
