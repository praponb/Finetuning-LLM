from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("city") == "London") | (F.col("opened_date") > "2023-10-01"))
    .agg(F.avg("sqft").alias("avg_sqft"))
    .orderBy("avg_sqft")
)
