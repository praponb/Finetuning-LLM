from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~((F.col("sqft") > 14988) & (F.col("store_name").like("%Airp%") | (F.col("sqft") >= 7849))))
    .groupBy("city")
    .agg(
        F.max("opened_date").alias("latest_opened_date"),
        F.count("*").alias("store_count"),
        F.min("sqft").alias("min_sqft"),
    )
    .orderBy(F.col("city").desc())
)
