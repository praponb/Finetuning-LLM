from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") == "London")
    .groupBy("region")
    .agg(F.max("sqft").alias("max_sqft"))
)
