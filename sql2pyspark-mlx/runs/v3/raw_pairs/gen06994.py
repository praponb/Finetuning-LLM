from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.max("units").alias("max_units"),
        F.round(F.stddev("units"), 2).alias("stddev_units"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.opened_date"),
        F.col("t.max_units"),
        F.col("t.stddev_units"),
    )
    .orderBy("opened_date")
)
