from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "id",
        "opened_date",
        "city",
        "store_name",
        F.upper("city").alias("city_upper"),
        F.upper(F.trim("store_name")).alias("store_name_normalized"),
    )
)
