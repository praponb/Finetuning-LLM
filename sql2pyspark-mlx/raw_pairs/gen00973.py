from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["North", "South"]) & (F.col("city") == "London"))
    .groupBy("city", F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
    )
    .filter(F.col("stddev_sqft") >= 72)
)
