from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(
        F.count("sqft").alias("sqft_count"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("sqft_count") >= 4)
)
