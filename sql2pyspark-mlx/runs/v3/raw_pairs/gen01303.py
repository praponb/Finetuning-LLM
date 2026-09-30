from pyspark.sql import functions as F

result = (
    df_stores
    .filter((~F.col("store_name").like("Ri%") & (F.col("sqft") == 6808)) | (F.col("region") == "Central"))
    .select(
        "id",
        "sqft",
        F.trim("region").alias("clean_region"),
        F.concat_ws(",", "region", "city").alias("region_city_key"),
    )
)
