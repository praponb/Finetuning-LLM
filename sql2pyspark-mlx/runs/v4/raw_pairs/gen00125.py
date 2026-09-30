from pyspark.sql import functions as F

filtered = (
    df_stores
    .filter(F.col("opened_date") >= F.lit("2023-07-10").cast("date"))
    .withColumn("city_title", F.initcap(F.trim("city")))
)

result = (
    filtered
    .groupBy("city")
    .pivot("region", ["Central", "North"])
    .agg(F.sum("sqft").alias("value_sum"))
)
