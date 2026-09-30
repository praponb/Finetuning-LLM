from pyspark.sql import functions as F

store_summary = (
    df_orders
    .filter(F.col("store_id").isin([12, 20, 36]))
    .groupBy("store_id")
    .agg(
        F.min("discount").alias("min_discount"),
        F.countDistinct("discount").alias("unique_channels"),
    )
)

result = (
    df_stores.alias("stores")
    .join(store_summary.alias("store_summary"), F.col("stores.id") == F.col("store_summary.store_id"), "inner")
    .select(
        F.col("stores.region"),
        F.col("store_summary.min_discount"),
        F.col("store_summary.unique_channels"),
    )
)
