from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.partitionBy("key")
w3 = Window.partitionBy("key").orderBy(F.col("latency_ms_share").asc_nulls_first())
w4 = Window.partitionBy("key", "service")
w5 = Window.partitionBy("service", "host").orderBy("host_stddev_samp_latency_ms_share")
w6 = Window.orderBy(F.col("latency_ms_share").asc_nulls_last())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        F.explode_outer("context").alias("key", "value"),
    )
)

cleaned = (
    exploded
    .filter(F.col("service") == "search")
    .select(
        "*",
        F.round("latency_ms", 0).alias("latency_ms_rounded"),
        F.concat("host", F.lit(" "), "key").alias("host_key"),
    )
)

remaining_keys = (
    cleaned
    .select("service", "host")
    .intersect(
        df_app_logs
        .filter(~F.col("host").isin(["h1", "h3"]))
        .select("service", "host")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.service") == F.col("k.service")) & (F.col("s.host") == F.col("k.host")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("key").isNotNull())
    .withColumn("clean_key", F.trim("key"))
    .withColumn("key_clean", F.coalesce("key", F.lit("N/A")))
)

enriched = (
    base
    .withColumn("host_stddev_samp_latency_ms", F.stddev_samp("latency_ms").over(w))
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4))
)

metrics = (
    enriched
    .select(
        "*",
        F.stddev_samp("latency_ms_share").over(w).alias("host_stddev_samp_latency_ms_share"),
        F.stddev_samp("latency_ms_share").over(w2).alias("key_stddev_samp_latency_ms_share"),
        F.percent_rank().over(w3).alias("latency_ms_share_percent_rank"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.count("latency_ms_share").over(w4).alias("key_count_latency_ms_share"),
        F.percent_rank().over(w5).alias("host_stddev_samp_latency_ms_share_percent_rank"),
        F.dense_rank().over(w6).alias("latency_ms_share_dense_rank"),
    )
)

result = (
    scored
    .rollup("service")
    .agg(
        F.count(F.when(F.col("host") == "h3", 1)).alias("h3_count"),
        F.bit_or("latency_ms").alias("latency_ms_bit_or"),
    )
    .orderBy(F.col("service").desc_nulls_first())
)
