from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "city",
        F.date_trunc("week", "opened_date").alias("opened_date_period"),
        F.concat_ws("/", "city", "region").alias("city_region_key"),
    )
    .orderBy(F.col("city_region_key").desc())
)
