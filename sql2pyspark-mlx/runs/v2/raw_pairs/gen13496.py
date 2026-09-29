from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country", "city")
    .agg(
        F.min("rating").alias("min_rating"),
        F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0)).alias("bangkok_count"),
    )
    .filter(F.col("bangkok_count") < 32)
    .drop("bangkok_count")
    .orderBy(F.col("min_rating").desc())
)
