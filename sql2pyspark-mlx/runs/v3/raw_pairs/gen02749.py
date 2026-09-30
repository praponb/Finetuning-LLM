from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "id",
        "store_name",
        "city",
        F.length("store_name").alias("store_name_length"),
    )
    .orderBy("city")
)
