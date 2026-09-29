from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("region") != "Central") & (F.col("region") == "Central") & (F.col("sqft") <= 14794))
    .select(
        "id",
        "city",
        F.concat_ws("-", "store_name", "city").alias("store_name_city_key"),
    )
)
