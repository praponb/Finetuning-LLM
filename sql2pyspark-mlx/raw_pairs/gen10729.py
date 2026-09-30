from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("latency_ms").asc_nulls_last())
w2 = Window.partitionBy("host", "level")
w3 = Window.partitionBy("k")
w4 = Window.partitionBy("service", "host")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("k", "value"),
    )
)

cleaned = (
    exploded
    .filter(F.col("k").isNotNull() & (~F.col("k").eqNullSafe(F.lit("trace"))))
    .withColumn("service_title", F.initcap(F.trim("service")))
)

enriched = (
    cleaned
    .select(
        "host",
        "latency_ms",
        "value",
        F.percent_rank().over(w).alias("latency_ms_percent_rank"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4).alias("latency_ms_share"),
        F.stddev_samp("latency_ms").over(w3).alias("k_stddev_samp_latency_ms"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w4)), 4).alias("latency_ms_share_2"),
    )
)

valid = (
    enriched
    .filter(F.col("latency_ms_percent_rank") <= 0.51)
    .select(
        "host",
        "latency_ms",
        "latency_ms_percent_rank",
        "latency_ms_share",
        "k_stddev_samp_latency_ms",
        "latency_ms_share_2",
        F.least("k_stddev_samp_latency_ms", "latency_ms_share").alias("least_k_stddev_samp_latency_ms_latency_ms_share"),
    )
)

result = (
    valid
    .rollup(
        F.when(F.col("latency_ms_share") >= 0.69, "HIGH").otherwise("LOW").alias("latency_ms_share_tier"),
        "host",
    )
    .agg(
        F.countDistinct("host").alias("distinct_hosts"),
        F.size(F.collect_set("host")).alias("num_hosts"),
        F.sort_array(F.collect_set("host")).alias("host_list"),
    )
    .filter(F.col("num_hosts") > 1)
    .orderBy(F.col("num_hosts").desc())
)
