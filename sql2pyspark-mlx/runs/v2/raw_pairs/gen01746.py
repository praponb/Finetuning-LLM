from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("o.store_id") == 37)
    .groupBy(F.col("s.region"))
    .agg(
        F.count(F.col("o.id")).alias("order_count"),
        F.sort_array(F.collect_set(F.col("o.channel"))).alias("channel_list"),
    )
    .filter(F.col("order_count") > 2)
    .orderBy("region")
)
