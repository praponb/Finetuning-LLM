from pyspark.sql import functions as F

cleaned = (
    df_stores
    .filter(F.col("sqft") <= 15492)
    .withColumn("store_name_upper", F.upper("store_name"))
    .withColumn("sqft_rounded", F.round("sqft", 1))
)

result = (
    cleaned
    .groupBy("city")
    .pivot("region", ["South", "North"])
    .agg(F.avg("sqft_rounded").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
)
