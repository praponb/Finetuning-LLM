from pyspark.sql import functions as F

cleaned = (
    df_stores
    .filter(F.col("opened_date") >= F.lit("2023-07-14").cast("date"))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        F.weekofyear("opened_date").alias("opened_date_weekofyear"),
        F.concat_ws("-", "city", "region").alias("city_region_key"),
    )
)

result = (
    cleaned
    .select("city", "id")
    .subtract(
        df_stores
        .filter(F.length("store_name") < 14)
        .select("city", "id")
    )
)
