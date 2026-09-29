from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["West", "North", "Central"]))
    .groupBy("region", F.month("opened_date").alias("opened_month"))
    .agg(
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.count("*").alias("store_count"),
    )
    .filter(F.col("store_count") > 4)
    .drop("store_count")
)
