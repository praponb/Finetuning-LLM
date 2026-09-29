from pyspark.sql import functions as F

base = (
    df_stores
    .filter(F.col("region").isNotNull())
    .withColumn("region_city_key", F.concat_ws("-", "region", "city"))
    .withColumn("region_filled", F.coalesce("region", F.lit("NONE")))
)

result = (
    base
    .groupBy("city")
    .pivot("region", ["Central", "West"])
    .agg(F.max("sqft"))
    .select("city", F.col("Central").alias("central"), F.col("West").alias("west"))
)
