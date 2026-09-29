from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(
        F.max("opened_date").alias("last_opened_date"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
    )
    .filter(F.col("avg_sqft") >= 68)
    .orderBy("city")
    .limit(10)
)
