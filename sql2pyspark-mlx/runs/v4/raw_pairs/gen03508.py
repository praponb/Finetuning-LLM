from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region", "city")
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.max("opened_date").alias("latest_opened_date"),
    )
    .filter(F.col("avg_sqft") > 11900)
    .orderBy(F.col("latest_opened_date").desc(), F.col("region").desc(), "city")
    .limit(20)
)
