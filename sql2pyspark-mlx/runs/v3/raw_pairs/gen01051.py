from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "id",
        "store_name",
        F.coalesce("store_name", F.lit("unassigned")).alias("store_name_filled"),
        F.substring("region", 1, 4).alias("region_prefix"),
    )
)
