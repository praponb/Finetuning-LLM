from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.count("units").alias("units_count"),
        F.round(F.stddev("units"), 2).alias("stddev_units"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.opened_date"),
        F.col("s.city"),
        F.col("t.units_count"),
        F.col("t.stddev_units"),
    )
)
