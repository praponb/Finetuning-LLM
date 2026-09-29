from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "service").orderBy("latency_ms")
w2 = Window.partitionBy("k")
w_recent = Window.orderBy(F.col("latency_ms_share").asc_nulls_last())
w_part = Window.partitionBy("host")
w_hist = Window.orderBy(F.col("latency_ms_rank").desc_nulls_first())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        F.explode_outer("context").alias("k", "value"),
    )
)

valid = (
    exploded
    .filter(~F.col("host").isin(["h4", "h2", "h1"]))
    .withColumn("latency_ms_num", F.col("latency_ms").cast("string"))
)

cleaned = (
    valid
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & F.col("k").isNotNull())
    .withColumn("host_service_key", F.concat_ws("-", "host", "service"))
)

enriched = (
    cleaned
    .select(
        "id",
        "host",
        "latency_ms",
        "latency_ms_num",
        F.rank().over(w).alias("latency_ms_rank"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4).alias("latency_ms_share"),
    )
)

scored = (
    enriched
    .select(
        "host",
        "latency_ms_rank",
        F.percent_rank().over(w_recent).alias("latency_ms_share_percent_rank"),
        F.percent_rank().over(w_recent).alias("latency_ms_share_percent_rank_2"),
        F.max("latency_ms").over(w_part).alias("host_max_latency_ms"),
        F.cume_dist().over(w_hist).alias("latency_ms_rank_cume_dist"),
    )
)

filtered = (
    scored
    .filter(F.col("latency_ms_share_percent_rank") <= 0.68)
    .select(
        "host",
        "latency_ms_rank",
        "latency_ms_share_percent_rank",
        "latency_ms_share_percent_rank_2",
        "host_max_latency_ms",
        "latency_ms_rank_cume_dist",
        F.round((F.col("latency_ms_share_percent_rank") * 0.85), 2).alias("latency_ms_share_percent_rank_scaled"),
        F.col("latency_ms_share_percent_rank_2").cast("int").alias("latency_ms_share_percent_rank_2_int"),
    )
)

result = (
    filtered
    .withColumn("latency_ms_rank_tier", F.when(F.col("latency_ms_rank") >= 7, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["host", "latency_ms_rank_tier"], ["host"], []],
        "host",
        "latency_ms_rank_tier",
    )
    .agg(
        F.countDistinct("host").alias("distinct_hosts"),
        F.round(F.avg("latency_ms_rank"), 2).alias("avg_latency_ms_rank"),
        F.sort_array(F.collect_set("host")).alias("host_list"),
        F.max(F.when(F.col("host") == "h1", F.col("latency_ms_rank_cume_dist"))).alias("h1_max_latency_ms_rank_cume_dist"),
        F.count("*").alias("cnt"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("avg_latency_ms_rank") > 0)
    .orderBy(F.col("avg_latency_ms_rank").desc_nulls_first())
)
