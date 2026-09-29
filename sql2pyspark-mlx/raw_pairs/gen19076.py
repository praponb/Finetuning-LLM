from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w_grp = Window.partitionBy("level")
w3 = Window.orderBy(F.col("status_code_high").desc())
w4 = Window.partitionBy("level").orderBy("flag_bit1")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

unpacked = (
    flattened
    .select(
        "host",
        "level",
        "latency_ms",
        "status_code",
        "value",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("flag_bit1"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

kept_keys = (
    unpacked
    .select("level", "host")
    .intersectAll(
        df_app_logs
        .filter(F.col("level") != "INFO")
        .select("level", "host")
    )
)

reconciled = (
    unpacked
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.host") == F.col("k.host")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .select(
        "*",
        F.min("status_code_high").over(w).alias("host_min_status_code_high"),
        F.avg("flag_bit1").over(w_grp).alias("level_avg_flag_bit1"),
        F.cume_dist().over(w3).alias("status_code_high_cume_dist"),
    )
)

enriched_app_logs = (
    enriched
    .select(
        "host",
        "level",
        "value",
        "flag_bit1",
        "level_avg_flag_bit1",
        "status_code_high_cume_dist",
        F.max("status_code_high").over(w).alias("host_max_status_code_high"),
        F.max("level_avg_flag_bit1").over(w_grp).alias("level_max_level_avg_flag_bit1"),
        F.cume_dist().over(w4).alias("flag_bit1_cume_dist"),
    )
)

result = (
    enriched_app_logs
    .groupBy(
        "level",
        F.when(F.col("status_code_high_cume_dist") >= 0.87, "PREMIUM").otherwise("STANDARD").alias("status_code_high_cume_dist_tier"),
    )
    .agg(
        F.sum(F.when(F.col("host") == "h2", F.col("level_avg_flag_bit1")).otherwise(0)).alias("h2_level_avg_flag_bit1"),
        F.count(F.when(F.col("level") == "WARN", 1)).alias("warn_count"),
        F.percentile_approx("flag_bit1_cume_dist", 0.95).alias("flag_bit1_cume_dist_p95"),
        F.percentile_approx("flag_bit1", 0.9).alias("flag_bit1_p90"),
    )
    .orderBy(F.col("flag_bit1_p90").desc())
)
