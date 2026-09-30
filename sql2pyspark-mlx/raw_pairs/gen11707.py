from pyspark.sql import functions as F

store_stats = (
    df_orders
    .filter(F.col("order_date") >= "2025-07-30")
    .groupBy("store_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.min("amount").alias("min_amount"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.region"),
        F.col("s.opened_date"),
        F.col("ss.status_values"),
        F.col("ss.min_amount"),
    )
    .orderBy(F.col("opened_date").desc(), "id")
    .limit(1)
)
