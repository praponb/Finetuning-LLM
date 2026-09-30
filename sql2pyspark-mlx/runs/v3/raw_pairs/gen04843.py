from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("o.store_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.id"), F.col("s.region"))
    .agg(
        F.count(F.col("o.id")).alias("order_count"),
        F.sort_array(F.collect_set(F.col("o.channel"))).alias("channel_list"),
        F.sum(F.when(F.col("o.channel") == "store", F.col("o.amount")).otherwise(0)).alias("store_amount"),
    )
    .orderBy(F.col("store_amount").desc())
)
