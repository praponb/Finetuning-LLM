from pyspark.sql import functions as F

src = (
    df_stores
    .filter((F.length("store_name") < 5) & F.col("region").isin(["West", "North", "Central"]))
    .withColumn("region_upper", F.upper("region"))
)

city_summary = (
    src
    .groupBy("city")
    .agg(F.count("sqft").alias("sqft_count"))
)

result = (
    city_summary
    .filter(F.col("sqft_count") >= 9)
    .select("city", "sqft_count")
    .orderBy(F.col("sqft_count").desc(), F.col("city").desc())
    .limit(5)
)
