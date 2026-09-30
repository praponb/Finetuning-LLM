from pyspark.sql import functions as F

result = (
    df_stores
    .agg(F.sum("sqft").alias("total_sqft"))
    .orderBy(F.col("total_sqft").desc())
)
