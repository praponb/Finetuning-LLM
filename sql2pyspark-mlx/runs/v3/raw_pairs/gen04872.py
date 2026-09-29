from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "left")
    .filter(~F.col("o.channel").isin(["store", "mobile"]))
    .groupBy(F.col("s.id"), F.col("s.city"))
    .agg(
        F.count(F.col("o.id")).alias("order_count"),
        F.sort_array(F.collect_set(F.col("o.channel"))).alias("channel_list"),
    )
    .orderBy(F.col("order_count").desc())
)
