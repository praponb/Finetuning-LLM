from pyspark.sql import functions as F

store_summary = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("ss.cnt") > 6)
    .select(
        F.col("s.id"),
        F.col("s.store_name"),
        F.col("ss.cnt"),
        F.col("ss.status_list"),
    )
    .orderBy("store_name")
)
