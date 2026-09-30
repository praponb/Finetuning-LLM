from pyspark.sql import functions as F

store_summary = (
    df_orders
    .filter(F.month("order_date").isin([2, 3, 5]))
    .groupBy("store_id")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.opened_date"),
        F.col("ss.status_list"),
    )
)
