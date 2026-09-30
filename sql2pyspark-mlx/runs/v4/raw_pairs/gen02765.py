from pyspark.sql import functions as F

valid = (
    df_stores
    .filter(F.col("opened_date") > F.lit("2024-02-27").cast("date"))
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        F.concat_ws("-", "city", "region").alias("city_region_key"),
        F.initcap(F.trim("region")).alias("region_title"),
        F.coalesce("region", F.lit("NONE")).alias("resolved_region"),
    )
)

result = (
    valid
    .select("city", "region")
    .subtract(
        df_stores
        .filter(F.col("region") == "Central")
        .select("city", "region")
    )
)
