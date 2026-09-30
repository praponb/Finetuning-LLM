from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") <= 8111) & F.col("store_name").like("%Old%"))
    .groupBy("region")
    .agg(F.min("sqft").alias("min_sqft"))
    .orderBy("region", F.col("min_sqft").desc())
)
