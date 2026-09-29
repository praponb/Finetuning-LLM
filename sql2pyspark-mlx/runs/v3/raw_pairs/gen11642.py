from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(F.min("sqft").alias("min_sqft"))
)
