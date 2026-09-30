from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("Ri%") | F.col("city").isin(["Tokyo", "Berlin", "Chiang Mai"]))
    .groupBy("city")
    .agg(
        F.min("sqft").alias("min_sqft"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"),
    )
    .orderBy(F.col("tokyo_count").desc(), F.col("min_sqft").desc())
)
