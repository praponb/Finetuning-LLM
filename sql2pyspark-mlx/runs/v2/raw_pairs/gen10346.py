from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "store_name",
        "sqft",
        "city",
        F.concat_ws("-", "city", "store_name").alias("city_store_name_key"),
    )
    .orderBy(F.col("city_store_name_key").desc(), F.col("sqft").desc())
)
