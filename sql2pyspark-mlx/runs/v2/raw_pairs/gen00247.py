from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("%Kio%"))
    .select(
        "opened_date",
        F.concat_ws("-", "city", "store_name").alias("city_store_name_key"),
    )
)
