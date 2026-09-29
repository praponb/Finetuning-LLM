from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("store_id") == 24) & F.col("discount").isNull())
    .groupBy("channel")
    .agg(F.sort_array(F.collect_set("channel")).alias("channel_list"))
    .orderBy(F.col("channel").desc())
    .limit(1)
)
