from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.partitionBy("level", "host")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

scored = lines.withColumn("level_avg_status_code", F.avg("status_code").over(w))

kept_keys = (
    scored
    .select("level", "host")
    .subtract(
        df_app_logs
        .filter(F.col("host").isin(["h3", "h2", "h1"]))
        .select("level", "host")
    )
)

verified = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.host") == F.col("k.host")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter(~F.col("k").eqNullSafe(F.lit("trace")))
    .select(
        "*",
        F.when(F.col("host").isin(["h3", "h1"]), "Group A").otherwise("Other").alias("host_group"),
        F.coalesce("k", F.lit("DIRECT")).alias("k_clean"),
    )
)

with_windows = (
    valid
    .select(
        "id",
        "level",
        "latency_ms",
        "k",
        "value",
        "host_group",
        "k_clean",
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4).alias("latency_ms_share"),
        F.count("level_avg_status_code").over(w2).alias("level_count_level_avg_status_code"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("latency_ms_share") >= 0.65, "HIGH").otherwise("LOW").alias("latency_ms_share_tier"),
        "k",
    )
    .agg(
        F.sum(F.when(F.col("level") == "DEBUG", F.col("latency_ms")).otherwise(0)).alias("debug_latency_ms"),
        F.percentile_approx("latency_ms_share", [0.25, 0.5, 0.75]).alias("latency_ms_share_quartiles"),
        F.round(F.avg("level_count_level_avg_status_code"), 2).alias("avg_level_count_level_avg_status_code"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.size(F.collect_set("k")).alias("num_ks"),
        F.sort_array(F.collect_set("k")).alias("k_list"),
    )
)
