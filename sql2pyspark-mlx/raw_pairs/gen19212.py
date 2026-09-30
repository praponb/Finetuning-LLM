from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").isNull() & (F.length("store_name") > 12))
    .groupBy(F.quarter("opened_date").alias("opened_quarter"), "city")
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.min("sqft").alias("min_sqft"),
    )
    .filter(F.col("min_sqft") < 11600)
)
