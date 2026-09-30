from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") >= 5179)
    .select(
        "store_name",
        "region",
        "city",
        F.concat_ws("/", "city", "region").alias("city_region_key"),
    )
    .orderBy("city_region_key")
)
