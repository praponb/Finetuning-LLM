from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") < "2023-04-01")
    .groupBy("region")
    .agg(F.max("sqft").alias("max_sqft"))
)
