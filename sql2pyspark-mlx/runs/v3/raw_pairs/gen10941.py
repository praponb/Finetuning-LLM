from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "region",
        F.length("region").alias("region_length"),
        F.trim("store_name").alias("clean_store_name"),
    )
)
