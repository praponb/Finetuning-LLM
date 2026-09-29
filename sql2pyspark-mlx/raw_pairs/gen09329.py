from pyspark.sql import functions as F

valid_stores = (
    df_stores
    .filter((F.col("city") == "Bangkok") & F.col("store_name").isNotNull() & (F.col("opened_date") > "2024-02-09"))
)

city_summary = (
    valid_stores
    .groupBy("city")
    .agg(F.round(F.stddev("sqft"), 2).alias("stddev_sqft"))
)

result = (
    city_summary
    .filter(F.col("stddev_sqft") < 27)
    .select("city", "stddev_sqft")
    .orderBy(F.col("stddev_sqft").desc())
)
