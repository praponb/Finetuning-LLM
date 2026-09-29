from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("service")
w2 = Window.partitionBy(F.month("log_ts"), "host").orderBy(F.col("latency_ms").asc_nulls_first())
w3 = Window.partitionBy("service", "host").orderBy("latency_ms")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("key", "v"),
    )
)

matched_keys = (
    exploded
    .select("id", "host")
    .intersectAll(
        df_app_logs
        .filter(F.col("level").isin(["ERROR", "INFO"]))
        .select("id", "host")
    )
)

kept_rows = (
    exploded
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.host") == F.col("k.host")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .select(
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "key",
        "v",
        F.sum("latency_ms").over(w_grp).alias("service_sum_latency_ms"),
    )
)

with_windows = (
    enriched
    .select(
        "key",
        "v",
        "service_sum_latency_ms",
        F.percent_rank().over(w2).alias("latency_ms_percent_rank"),
        F.percent_rank().over(w3).alias("latency_ms_percent_rank_2"),
    )
)

base = (
    with_windows
    .filter(F.col("key").eqNullSafe(F.lit("region")))
    .select(
        "*",
        F.when(F.col("key") == "trace", 1).otherwise(0).alias("is_trace"),
        F.coalesce("key", F.lit("DIRECT")).alias("key_filled"),
    )
)

result = (
    base
    .withColumn("is_trace_tier", F.when(F.col("is_trace") >= 89, "PREMIUM").otherwise("STANDARD"))
    .groupingSets([["key", "is_trace_tier"], ["key"], []], "key", "is_trace_tier")
    .agg(
        F.count_if(F.col("key") == "version").alias("version_events"),
        F.percentile_approx("is_trace", [0.25, 0.5, 0.75]).alias("is_trace_quartiles"),
        F.countDistinct("key").alias("distinct_keys"),
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.round(F.sum("latency_ms_percent_rank") / F.nullif(F.count(F.when(F.col("key") == "trace", 1)), F.lit(0)), 2).alias("latency_ms_percent_rank_per_trace"),
        F.count(F.when(F.col("key") == "version", 1)).alias("version_rows"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("is_trace_tier").desc())
)
