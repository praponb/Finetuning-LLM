from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") >= "2023-11-20")
    .groupBy("region", F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(F.round(F.avg("sqft"), 2).alias("avg_sqft"))
    .filter(F.col("avg_sqft") >= 63)
)
