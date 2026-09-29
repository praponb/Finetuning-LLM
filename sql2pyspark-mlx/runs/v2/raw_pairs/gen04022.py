from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"),
        F.min("opened_date").alias("first_opened_date"),
        F.min("sqft").alias("min_sqft"),
    )
    .filter(F.col("min_sqft") >= 7600)
    .drop("min_sqft")
    .orderBy("city")
)
