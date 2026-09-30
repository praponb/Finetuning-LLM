from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "id",
        "region",
        "opened_date",
        F.lower("store_name").alias("store_name_lower"),
        F.coalesce("store_name", F.lit("N/A")).alias("store_name_filled"),
    )
    .orderBy("region")
)
