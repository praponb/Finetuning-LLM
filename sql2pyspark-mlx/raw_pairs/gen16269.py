from pyspark.sql import functions as F

rollup_totals = (
    df_inventory
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "left")
    .rollup(F.col("d.region").alias("region"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("src.quantity")), 2).alias("total_quantity"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    rollup_totals
    .select(
        "region",
        "cnt",
        "total_quantity",
        "gid",
        F.when(F.col("region").rlike("^(North|Central)$"), "CORE").otherwise("GROUP_B").alias("region_zone"),
    )
    .orderBy(F.col("total_quantity").asc_nulls_first())
)
