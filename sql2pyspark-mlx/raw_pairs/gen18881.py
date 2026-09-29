from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") <= 6456)
    .groupBy("city", F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(F.round(F.avg("sqft"), 2).alias("avg_sqft"))
    .filter(F.col("avg_sqft") >= 73)
    .select("city", "opened_month", "avg_sqft")
    .orderBy("avg_sqft", F.col("opened_month").desc(), "city")
    .limit(5)
)
