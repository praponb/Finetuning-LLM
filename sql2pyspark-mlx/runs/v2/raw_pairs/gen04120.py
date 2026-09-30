from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .filter(F.col("sale_date") > "2025-05-01")
    .groupBy("store_id")
    .agg(
        F.min("units").alias("min_units"),
        F.round(F.stddev("units"), 2).alias("stddev_units"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("S"), F.col("s.id") == F.col("S.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("S.min_units"),
        F.col("S.stddev_units"),
    )
    .orderBy("city")
)
