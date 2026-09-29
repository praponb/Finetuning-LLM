from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("store_name").like("%Mal%"))
    .select(
        "region",
        "opened_date",
        "store_name",
        F.coalesce("store_name", F.lit("N/A")).alias("store_name_filled"),
    )
)
