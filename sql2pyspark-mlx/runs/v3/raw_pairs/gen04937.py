from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.city"))
    .agg(
        F.count(F.col("o.id")).alias("order_count"),
        F.sort_array(F.collect_set(F.col("o.channel"))).alias("channel_list"),
    )
)
