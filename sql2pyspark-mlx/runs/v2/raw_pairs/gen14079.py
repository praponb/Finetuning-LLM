from pyspark.sql import functions as F

store_totals = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .filter(F.col("t.stddev_discount") >= 78)
    .select(F.col("s.sqft"), F.col("t.stddev_discount"), F.col("t.channel_list"))
    .orderBy(F.col("stddev_discount").desc(), F.col("sqft").desc())
)
