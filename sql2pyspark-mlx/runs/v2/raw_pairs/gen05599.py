from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("city") == "Tokyo", F.col("rating")).otherwise(0)).alias("tokyo_rating"),
        F.max("rating").alias("max_rating"),
    )
    .filter(F.col("max_rating") < 2.9)
    .orderBy(F.col("tokyo_rating").desc())
)
