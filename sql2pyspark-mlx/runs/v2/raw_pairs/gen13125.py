from pyspark.sql import functions as F

store_summary = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.min("order_date").alias("min_order_date"),
        F.count("*").alias("num_orders"),
    )
)

result = (
    df_stores.alias("stores")
    .join(store_summary.alias("StoreSummary"), F.col("stores.id") == F.col("StoreSummary.store_id"), "inner")
    .select(
        F.col("stores.id"),
        F.col("stores.region"),
        F.col("StoreSummary.min_order_date"),
        F.col("StoreSummary.num_orders"),
    )
)
