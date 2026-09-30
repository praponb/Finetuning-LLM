from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "id",
        "opened_date",
        "store_name",
        "city",
        F.length("city").alias("city_length"),
    )
)
