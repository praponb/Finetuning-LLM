from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "id",
        "region",
        "opened_date",
        F.concat_ws("-", "store_name", "region").alias("store_name_region_key"),
    )
    .orderBy("opened_date")
)
