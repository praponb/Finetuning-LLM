from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["East", "West", "North"]))
    .select(
        "id",
        "opened_date",
        F.concat("region", F.lit("/"), "city").alias("region_city"),
        F.lower("city").alias("city_lower"),
    )
)
