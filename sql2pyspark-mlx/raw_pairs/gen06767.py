from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "id",
        "region",
        "opened_date",
        F.coalesce("store_name", F.lit("unassigned")).alias("store_name_filled"),
    )
    .orderBy("region", "store_name_filled")
)
