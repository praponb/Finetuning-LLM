from pyspark.sql import functions as F

recent_stores = (
    df_stores
    .filter((F.lower(F.trim("store_name")) == "airport kiosk") & (F.col("sqft") != 15141) & (F.col("opened_date") < "2024-04-26"))
)

agg = (
    recent_stores
    .groupBy("city")
    .agg(F.round(F.stddev("sqft"), 2).alias("stddev_sqft"))
)

result = (
    agg
    .filter(F.col("stddev_sqft") < 63)
    .orderBy("city")
)
