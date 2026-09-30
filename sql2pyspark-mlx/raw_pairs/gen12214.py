from pyspark.sql import functions as F

filtered_stores = (
    df_stores
    .filter(F.col("city").isin(["New York", "Tokyo"]))
    .withColumn("store_name_prefix", F.substring("store_name", 1, 4))
)

region_summary = (
    filtered_stores
    .groupBy("region")
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.count("sqft").alias("sqft_count"),
    )
)

result = (
    region_summary
    .filter(F.col("avg_sqft") < 11000)
    .orderBy(F.col("avg_sqft").desc())
)
