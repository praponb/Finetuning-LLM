from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.count("sqft").alias("sqft_count"),
    )
    .filter(F.col("sqft_count") > 9)
)
