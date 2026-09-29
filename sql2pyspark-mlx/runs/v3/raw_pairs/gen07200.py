from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.upper("store_name").like("%LD %") & (F.col("opened_date") < "2023-07-01"))
    .select(
        "id",
        "city",
        "opened_date",
        F.trim("store_name").alias("clean_store_name"),
        F.length("region").alias("region_length"),
    )
)
