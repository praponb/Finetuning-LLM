from pyspark.sql import functions as F

dim_totals = (
    df_store_sales
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["region", "city"], ["region"], []],
        F.col("p.region").alias("region"),
        F.col("p.city").alias("city"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("src.revenue")).alias("max_revenue"),
        F.round(F.avg(F.col("src.units")), 2).alias("avg_units"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    dim_totals
    .select(
        "region",
        "rows_total",
        "max_revenue",
        "avg_units",
        "gid",
        F.when(F.col("region").rlike("^(Central|West)$"), "GROUP_B").otherwise("PRIORITY").alias("region_zone"),
    )
    .orderBy(F.col("avg_units").desc_nulls_last())
)
