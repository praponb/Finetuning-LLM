from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(F.sum("sqft").alias("total_sqft"))
    .orderBy(F.col("city").desc())
)
