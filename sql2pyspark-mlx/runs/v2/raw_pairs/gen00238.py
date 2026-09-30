from pyspark.sql import functions as F

store_totals = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .filter(F.col("t.avg_amount") > 1180)
    .select(
        F.col("s.id"),
        F.col("s.opened_date"),
        F.col("t.avg_amount"),
        F.col("t.channel_values"),
    )
)
