from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region", "city")
    .agg(
        F.max("opened_date").alias("latest_opened_date"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
    )
    .orderBy(F.col("latest_opened_date").desc(), F.col("avg_sqft").desc())
)
