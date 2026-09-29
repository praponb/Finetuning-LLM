from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "sqft",
        F.coalesce("store_name", F.lit("unassigned")).alias("store_name_filled"),
    )
    .orderBy("store_name_filled")
)
