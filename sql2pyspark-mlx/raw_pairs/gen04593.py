from pyspark.sql import functions as F

order_stats = (
    df_shipments
    .filter(F.col("delivered_date") < "2023-06-10")
    .groupBy("order_id")
    .agg(
        F.min("cost").alias("min_cost"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .select(
        F.col("o.status"),
        F.col("o.store_id"),
        F.col("os.min_cost"),
        F.col("os.status_list"),
    )
    .orderBy(F.col("store_id").desc())
)
