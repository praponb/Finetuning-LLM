from pyspark.sql import functions as F

covered = (
    df_plays.alias("a")
    .join(
        df_plays.alias("b"),
        (F.col("b.listener_id") == F.col("a.listener_id"))
        & (F.col("b.played_at") < F.col("a.played_at"))
        & (F.col("b.played_at") >= F.col("a.played_at") - F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.listener_id", "a.played_at")
    .agg(
        F.count("b.id").alias("prior_rows_7d"),
        F.sum("b.ms_played").alias("prior_ms_played_7d"),
    )
)

line_level = (
    covered.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.played_at") < F.lit("2025-02-24 12:00:00").cast("timestamp")), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.played_at",
        "s.prior_rows_7d",
        "s.prior_ms_played_7d",
        F.col("p.id").alias("play_id"),
        F.col("p.track_id").alias("play_track_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.source").alias("play_source"),
    )
)

tagged = (
    line_level
    .filter(F.col("played_at") >= F.lit("2025-02-06 12:00:00").cast("timestamp"))
    .select(
        "*",
        F.concat(F.col("play_source"), F.lit("#"), F.col("prior_ms_played_7d").cast("string")).alias("play_source_ref"),
        F.date_trunc("hour", F.col("play_played_at")).alias("play_played_at_hour"),
    )
)

per_group = (
    tagged
    .groupBy("play_device", "prior_rows_7d")
    .agg(
        F.round(F.stddev("prior_rows_7d"), 2).alias("stddev_prior_rows_7d"),
        F.round(100 * F.count(F.when(F.col("play_device") == "car", 1)) / F.count("*"), 1).alias("pct_play_device_car"),
        F.count("*").alias("row_count"),
        F.round(F.sum("prior_rows_7d") / F.nullif(F.sum("prior_ms_played_7d"), F.lit(0)), 3).alias("prior_rows_7d_to_prior_ms_played_7d_ratio"),
        F.countDistinct("play_track_id").alias("distinct_play_track_ids"),
    )
)

result = (
    per_group
    .select(
        "play_device",
        "prior_rows_7d",
        "stddev_prior_rows_7d",
        "row_count",
        "distinct_play_track_ids",
        F.length(F.trim(F.col("play_device"))).alias("play_device_len"),
        F.length(F.trim(F.col("play_device"))).alias("play_device_len_2"),
    )
    .orderBy(
        F.col("row_count").desc_nulls_first(),
        F.col("stddev_prior_rows_7d").desc(),
    )
)
