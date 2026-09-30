from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(
        F.count("*").alias("row_count"),
        F.count("sqft").alias("sqft_count"),
    )
    .filter(F.col("sqft_count") >= 1)
)
