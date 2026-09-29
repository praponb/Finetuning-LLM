from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 4, 5]))
    .groupBy("store_id")
    .agg(
        F.count("units").alias("units_count"),
        F.round(F.stddev("units"), 2).alias("stddev_units"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("S"), F.col("s.id") == F.col("S.store_id"), "inner")
    .filter(F.col("S.units_count") >= 6)
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.store_name"),
        F.col("S.units_count"),
        F.col("S.stddev_units"),
    )
    .orderBy("store_name", F.col("stddev_units").desc())
)
