from pyspark.sql import functions as F

dim_totals = (
    df_store_sales
    .alias("src")
    .join(df_stores.alias("dim"), F.col("src.store_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["region", "city"], ["region"]],
        F.col("dim.region").alias("region"),
        F.col("dim.city").alias("city"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.col("src.units")).alias("total_units"),
        F.round(F.sum(F.col("src.revenue")), 2).alias("total_revenue"),
    )
)

result = (
    dim_totals
    .select(
        "region",
        "city",
        "cnt",
        "total_revenue",
        F.when(F.col("region").rlike("^(Central|South)$"), "EXTENDED").when(F.col("region").rlike("^(North|East)$"), "GROUP_B").otherwise("PRIORITY").alias("region_zone"),
    )
    .orderBy(F.col("city").desc_nulls_first())
)
