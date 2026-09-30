from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["South", "North", "East"]))
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(F.round(F.stddev("sqft"), 2).alias("stddev_sqft"))
    .filter(F.col("stddev_sqft") > 60)
    .select("opened_month", "stddev_sqft")
    .orderBy("opened_month", F.col("stddev_sqft").desc())
)
