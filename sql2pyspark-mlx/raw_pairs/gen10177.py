from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("log_ts", "id", "k")
w2 = Window.partitionBy("level").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("k").orderBy("log_ts", "id", "k")
w4 = Window.partitionBy("k").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("level").orderBy(F.col("status_code").desc())
w6 = Window.partitionBy("k").orderBy("status_code")
w7 = Window.partitionBy("k")
w8 = Window.partitionBy("level", "k")

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

prev_events = exploded.withColumn("prev_log_ts", F.lag("log_ts").over(w))

gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_log_ts").isNull() | (F.col("log_ts") > F.col("prev_log_ts") + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessions_app_logs = (
    gaps
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        "k",
        F.sum("session_start_flag").over(w2).alias("burst_id"),
    )
)

gaps_2 = (
    sessions_app_logs
    .withColumn("session_start_flag", F.when(F.lag("log_ts").over(w3).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w3) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = gaps_2.withColumn("island_id", F.sum("session_start_flag").over(w4))

valid = (
    sessionized
    .filter((F.col("log_ts") >= F.to_timestamp(F.lit("2025-06-01 15:00:00"))) & F.col("k").isNotNull())
    .select(
        "id",
        "level",
        "status_code",
        "k",
        "session_start_flag",
        "island_id",
        F.least("burst_id", "island_id").alias("least_burst_id_island_id"),
        F.when(F.col("session_start_flag") >= 1, "High").when(F.col("session_start_flag") >= 0, "Medium").otherwise("Low").alias("session_start_flag_tier"),
        F.coalesce("k", F.lit("NONE")).alias("k_filled"),
    )
)

kept_keys = (
    valid
    .select("id", "level")
    .intersect(
        df_app_logs
        .filter(F.col("level") == "WARN")
        .select("id", "level")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .filter(F.col("level") != "DEBUG")
    .select(
        "id",
        "session_start_flag",
        "session_start_flag_tier",
        F.percent_rank().over(w5).alias("status_code_percent_rank"),
        F.rank().over(w6).alias("status_code_rank"),
        F.min("island_id").over(w7).alias("k_min_island_id"),
        F.stddev_samp("status_code").over(w8).alias("level_stddev_samp_status_code"),
    )
)

result = (
    scored
    .groupBy(
        "k_min_island_id",
        F.when(F.col("session_start_flag") >= 1, "LARGE").otherwise("SMALL").alias("session_start_flag_tier"),
    )
    .agg(
        F.round(F.avg("status_code_percent_rank"), 2).alias("avg_status_code_percent_rank"),
        F.percentile_approx("session_start_flag", [0.25, 0.5, 0.75]).alias("session_start_flag_quartiles"),
        F.min("session_start_flag").alias("min_session_start_flag"),
    )
    .filter(F.col("min_session_start_flag") < 1000000)
    .orderBy(F.col("k_min_island_id").desc())
)
