from pyspark.sql import functions as F

enriched = (
    df_inventory
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.city"), F.col("p.opened_date"), F.col("p.store_name"))
)

result = (
    enriched
    .select(
        "id",
        "store_id",
        "opened_date",
        "store_name",
        F.when(F.col("store_name").rlike("^(Riverside|Downtown Express)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("store_name_zone"),
    )
    .orderBy(F.col("id").desc())
)
