from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .groupBy(F.col("s.city"))
    .agg(
        F.max(F.col("ss.units")).alias("max_units"),
        F.round(F.stddev(F.col("ss.units")), 2).alias("stddev_units"),
    )
    .orderBy(F.col("max_units").desc())
)
