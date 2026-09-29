from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.year("opened_date") >= 2024)
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(F.round(F.avg("sqft"), 2).alias("avg_sqft"))
    .filter(F.col("avg_sqft") >= 25)
    .select("opened_month", "avg_sqft")
    .orderBy(F.col("opened_month").desc())
)
