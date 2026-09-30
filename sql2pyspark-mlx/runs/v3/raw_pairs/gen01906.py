from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.year("opened_date") == 2025) & F.col("store_name").like("Sia%"))
    .select(
        "id",
        "sqft",
        "opened_date",
        "region",
        F.coalesce("store_name", F.lit("Unknown")).alias("store_name_filled"),
    )
    .orderBy("region")
)
