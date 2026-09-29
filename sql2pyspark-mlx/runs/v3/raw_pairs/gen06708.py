from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("A%"))
    .agg(F.count("sqft").alias("sqft_count"))
    .orderBy(F.col("sqft_count").desc())
)
