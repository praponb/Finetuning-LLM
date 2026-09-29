from pyspark.sql import functions as F

result = (
    df_store_sales
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "inner")
    .rollup(F.col("p.region").alias("region"))
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.avg(F.col("a.units")), 2).alias("avg_units"),
        F.avg(F.col("a.revenue")).alias("avg_revenue"),
        F.grouping_id().alias("level_id"),
    )
)
