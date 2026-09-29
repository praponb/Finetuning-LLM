from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city", F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(F.max("sqft").alias("max_sqft"))
    .orderBy("city")
)
