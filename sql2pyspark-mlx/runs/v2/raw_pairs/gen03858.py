from pyspark.sql import functions as F

result = (
    df_stores
    .agg(F.count("sqft").alias("sqft_count"))
    .orderBy(F.col("sqft_count").desc())
)
