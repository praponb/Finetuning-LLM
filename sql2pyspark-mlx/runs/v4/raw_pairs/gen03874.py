from pyspark.sql import functions as F

kept = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.last_restock") <= F.col("s.opened_date")), "left_semi")
)

result = (
    kept
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft",
        F.when(F.col("region").rlike("^(West|North)$"), "OTHER_ZONE").when(F.col("region").rlike("^(South|East)$"), "PRIORITY").otherwise("EXTENDED").alias("region_zone"),
    )
)
