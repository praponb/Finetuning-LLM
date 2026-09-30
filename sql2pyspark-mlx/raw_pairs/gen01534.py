from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["East", "West", "North"]))
    .groupBy("city")
    .agg(
        F.max("sqft").alias("max_sqft"),
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.avg("sqft").alias("avg_sqft"),
    )
    .orderBy("sqft_range", "city")
    .limit(5)
)
