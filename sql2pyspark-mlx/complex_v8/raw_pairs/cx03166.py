from pyspark.sql import functions as F

in_window = (
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

exploded_detail_plays = (
    in_window.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.device") != "car"), "left")
    .select(
        "s.id",
        "s.listener_id",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.source").alias("play_source"),
    )
)

with_parent = (
    exploded_detail_plays.alias("s")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "inner")
    .select("s.*", F.col("p.device").alias("play_device_2"))
)

tagged = (
    with_parent
    .filter(F.col("play_source") != "album")
    .select(
        "*",
        F.length(F.trim(F.col("play_device_2"))).alias("play_device_2_len"),
        F.coalesce(F.col("play_device"), F.lit("UNKNOWN")).alias("play_device_or_unknown"),
    )
)

result = (
    tagged
    .select(
        "id",
        "play_id",
        "play_device",
        "play_is_skipped",
        "play_played_at",
        "play_source",
        "play_device_2",
        "play_device_2_len",
    )
    .orderBy(F.col("play_device").desc())
)
