from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region", F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(F.min("sqft").alias("min_sqft"))
    .filter(F.col("min_sqft") > 15400)
    .orderBy("region", "min_sqft", "opened_month")
    .limit(1)
)
