from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(F.min("sqft").alias("min_sqft"))
    .orderBy("min_sqft")
)
