from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") < 0.15)
    .groupBy("channel")
    .agg(
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_count"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
    .filter(F.col("web_count") >= 8)
    .select("channel", "web_count", "channel_list")
    .orderBy(F.col("web_count").desc())
)
