from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").isNotNull())
    .select(
        "sqft",
        "city",
        "store_name",
        F.concat_ws("-", "store_name", "city").alias("store_name_city_key"),
        F.concat("region", F.lit(", "), "store_name").alias("region_store_name"),
    )
    .orderBy(F.col("sqft").desc(), "region_store_name")
)
