from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "left")
    .groupBy(F.col("s.region"))
    .agg(
        F.count(F.when(F.col("o.status") == "PENDING", 1)).alias("pending_count"),
        F.sort_array(F.collect_set(F.col("o.status"))).alias("status_list"),
    )
)
