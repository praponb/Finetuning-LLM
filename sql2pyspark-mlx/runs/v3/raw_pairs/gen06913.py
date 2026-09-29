from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("city").isin(["Bangkok", "New York"]) | F.col("store_name").isNotNull())
    .select("sqft", F.concat_ws("-", "city", "region").alias("city_region_key"))
    .orderBy(F.col("city_region_key").desc(), "sqft")
)
