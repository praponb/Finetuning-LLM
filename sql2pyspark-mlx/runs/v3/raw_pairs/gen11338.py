from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "region",
        F.concat_ws("-", "city", "store_name").alias("city_store_name_key"),
        F.dayofweek("opened_date").alias("opened_date_dayofweek"),
    )
)
