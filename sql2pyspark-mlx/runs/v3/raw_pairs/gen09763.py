from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "id",
        "region",
        "store_name",
        "city",
        F.concat_ws(",", "store_name", "city").alias("store_name_city_key"),
        F.upper("region").alias("region_upper"),
    )
)
