from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["Central", "West", "North"]))
    .select(
        "store_name",
        "city",
        "opened_date",
        F.coalesce("region", F.lit("Unknown")).alias("region_filled"),
    )
)
