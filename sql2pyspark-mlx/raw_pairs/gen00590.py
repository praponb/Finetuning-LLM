from pyspark.sql import functions as F

by_dimension = (
    df_store_sales
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "inner")
    .groupBy(F.col("d.region").alias("region"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("s.revenue")).alias("max_revenue"),
        F.round(F.sum(F.col("s.units")), 2).alias("total_units"),
    )
)

result = (
    by_dimension
    .select("region", "cnt")
    .orderBy("region")
)
