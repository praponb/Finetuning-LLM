from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("units"), 2).alias("stddev_units"),
        F.count("revenue").alias("revenue_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .filter(F.col("t.stddev_units") > 26)
    .select(
        F.col("s.id"),
        F.col("s.opened_date"),
        F.col("t.stddev_units"),
        F.col("t.revenue_count"),
    )
    .orderBy(F.col("revenue_count").desc())
)
