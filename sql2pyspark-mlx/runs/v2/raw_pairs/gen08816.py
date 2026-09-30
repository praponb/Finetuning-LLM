from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "id",
        "sqft",
        "city",
        "opened_date",
        F.concat("store_name", F.lit(" - "), "region").alias("store_name_region"),
        F.weekofyear("opened_date").alias("opened_date_weekofyear"),
    )
)
