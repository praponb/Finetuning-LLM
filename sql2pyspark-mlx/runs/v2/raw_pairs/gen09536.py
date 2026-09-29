from pyspark.sql import functions as F

filtered_stores = (
    df_stores
    .filter(((F.col("region") != "South") & (F.col("opened_date") >= "2024-04-01")) | F.col("store_name").like("%Downt%"))
)

summary = (
    filtered_stores
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(F.min("sqft").alias("min_sqft"))
)

result = (
    summary
    .select("opened_year", "min_sqft")
    .orderBy("opened_year")
)
