from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("%Exp%") & F.col("store_name").like("%Rive%") & (F.col("opened_date") < "2024-10-12"))
    .agg(F.min("sqft").alias("min_sqft"))
)
