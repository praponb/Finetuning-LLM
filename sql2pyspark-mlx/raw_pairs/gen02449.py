from pyspark.sql import functions as F

filtered_stores = df_stores.filter((F.col("sqft") == 14440) & ~F.col("store_name").like("%Airport%"))

summary = (
    filtered_stores
    .groupBy("region")
    .agg(
        F.count("*").alias("total_count"),
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
    )
)

result = (
    summary
    .filter(F.col("stddev_sqft") < 38)
    .select("region", "total_count", "stddev_sqft")
    .orderBy(F.col("total_count").desc())
)
