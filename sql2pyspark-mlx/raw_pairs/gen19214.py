from pyspark.sql import functions as F

dim_totals = (
    df_inventory
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "inner")
    .groupingSets(
        [["region", "city"], ["region"]],
        F.col("s.region").alias("region"),
        F.col("s.city").alias("city"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("e.quantity")).alias("max_quantity"),
        F.grouping_id().alias("level_id"),
    )
)

result = (
    dim_totals
    .select(
        "region",
        "city",
        "level_id",
        F.when(F.col("region").rlike("^(Central|North)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("region_zone"),
    )
)
