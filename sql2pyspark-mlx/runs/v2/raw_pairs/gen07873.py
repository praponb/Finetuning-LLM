from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("city") == "Tokyo", F.col("rating")).otherwise(0)).alias("tokyo_rating"),
        (F.max("rating") - F.min("rating")).alias("rating_range"),
    )
    .filter(F.col("tokyo_rating") < 39)
    .orderBy(F.col("rating_range").desc())
)
