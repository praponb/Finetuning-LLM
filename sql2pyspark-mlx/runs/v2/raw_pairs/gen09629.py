from pyspark.sql import functions as F

store_summary = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.countDistinct("shipping_fee").alias("unique_channels"),
        F.count("*").alias("order_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.store_name"),
        F.col("ss.unique_channels"),
        F.col("ss.order_count"),
    )
    .orderBy(F.col("unique_channels").desc())
)
