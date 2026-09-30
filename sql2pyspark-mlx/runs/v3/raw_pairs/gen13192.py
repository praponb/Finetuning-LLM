from pyspark.sql import functions as F

filtered_stores = df_stores.filter(F.col("opened_date") > "2023-08-01")

stores_stats = (
    filtered_stores
    .groupBy("city")
    .agg(
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
    )
)

result = (
    stores_stats
    .filter(F.col("stddev_sqft") > 34)
    .select("city", "sqft_range")
)
