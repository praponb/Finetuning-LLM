from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "id",
        "sqft",
        "city",
        F.substring("city", 1, 4).alias("city_prefix"),
        F.length("region").alias("region_length"),
    )
    .orderBy("region_length", "sqft")
)
