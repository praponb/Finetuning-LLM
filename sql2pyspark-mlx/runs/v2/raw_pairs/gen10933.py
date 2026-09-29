from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("Si%") & (F.col("city") != "Bangkok"))
    .groupBy("city")
    .agg(
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.count("*").alias("store_count"),
    )
    .filter(F.col("store_count") < 4)
    .orderBy("city")
    .limit(10)
)
