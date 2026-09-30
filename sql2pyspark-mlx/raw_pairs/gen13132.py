from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region", F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
    )
    .filter(F.col("stddev_sqft") > 78)
)
