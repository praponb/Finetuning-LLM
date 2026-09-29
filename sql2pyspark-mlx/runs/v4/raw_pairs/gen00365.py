from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "id",
        "location",
        F.substring("location", 1, 4).alias("location_prefix"),
        F.coalesce("location", F.lit("Unknown")).alias("location_filled"),
    )
)
