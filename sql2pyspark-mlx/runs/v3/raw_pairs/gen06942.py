from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["Bangkok", "Berlin", "Singapore"]) & (F.col("opened_date") > "2024-04-01"))
    .select(
        "store_name",
        "city",
        "sqft",
        F.initcap(F.trim("city")).alias("city_title"),
        F.concat_ws(",", "city", "region").alias("city_region_key"),
    )
)
