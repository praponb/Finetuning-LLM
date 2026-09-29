from pyspark.sql import functions as F

base = (
    df_stores
    .filter(F.col("opened_date") > F.lit("2023-04-10").cast("date"))
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        F.concat_ws(",", "region", "store_name").alias("region_store_name_key"),
        F.ceil("sqft").alias("sqft_whole"),
    )
)

result = (
    base
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") >= F.col("s.opened_date")), "left_semi")
)
