from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["East", "Central"]))
    .groupBy(
        F.date_format("opened_date", "yyyy-MM").alias("opened_month"),
        F.when(F.col("sqft") >= 13279, "high").when(F.col("sqft") >= 5964, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.max("opened_date").alias("latest_opened_date"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
    )
    .filter(F.col("avg_sqft") > 25)
    .orderBy("latest_opened_date", "opened_month")
)
