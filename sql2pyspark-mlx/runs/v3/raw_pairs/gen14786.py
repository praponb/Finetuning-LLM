from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") < "2023-06-01")
    .groupBy("city")
    .agg(F.max("sqft").alias("max_sqft"))
    .filter(F.col("max_sqft") > 7600)
    .select("city", "max_sqft")
)
