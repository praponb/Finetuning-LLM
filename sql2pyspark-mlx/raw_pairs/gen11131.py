from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.partitionBy("attr_key", "host")
w3 = Window.partitionBy("host")
w4 = Window.orderBy(F.col("latency_ms").desc_nulls_first())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

scored = (
    lines
    .withColumn("latency_ms_avg_2h", F.avg("latency_ms").over(w))
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4))
)

with_windows = (
    scored
    .select(
        "host",
        "log_ts",
        "latency_ms",
        "v",
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w3)), 4).alias("latency_ms_share_2"),
        F.dense_rank().over(w4).alias("latency_ms_dense_rank"),
    )
)

base = (
    with_windows
    .filter(F.col("log_ts") > F.lit("2025-06-01 11:00:00").cast("timestamp"))
    .select(
        "*",
        F.round("latency_ms_dense_rank", 1).alias("latency_ms_dense_rank_rounded"),
        F.col("latency_ms_share_2").cast("int").alias("latency_ms_share_2_int"),
    )
)

stats = (
    base
    .withColumn("latency_ms_dense_rank_tier", F.when(F.col("latency_ms_dense_rank") >= 9, "PREMIUM").otherwise("STANDARD"))
    .cube("latency_ms_dense_rank_tier", "host")
    .agg(
        F.count(F.when(F.col("host") == "h1", 1)).alias("h1_rows"),
        F.count_if(F.col("host") == "h3").alias("h3_events"),
        F.size(F.collect_set("host")).alias("num_hosts"),
        F.round(F.avg("latency_ms_share_2_int"), 2).alias("avg_latency_ms_share_2_int"),
        F.count_if(F.col("host") == "h1").alias("h1_events"),
        F.grouping("latency_ms_dense_rank_tier").alias("grouping_level"),
    )
    .filter(F.col("h3_events") < 1000)
)

result = (
    stats
    .select(
        "latency_ms_dense_rank_tier",
        "host",
        "h3_events",
        "avg_latency_ms_share_2_int",
        "h1_events",
        F.when(F.col("host").rlike("^(h1|h3)$"), "EXTENDED").when(F.col("host").rlike("^(h2|h4)$"), "CORE").otherwise("PRIORITY").alias("host_zone"),
    )
    .orderBy("avg_latency_ms_share_2_int")
)
