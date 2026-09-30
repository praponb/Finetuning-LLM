from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("store_name").like("%Tow%") & (F.col("opened_date") >= "2023-06-01")) | F.col("store_name").like("%Flagshi%"))
    .agg(
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.count("sqft").alias("sqft_count"),
    )
)
