from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("play_listener_id").orderBy(F.col("play_played_at").desc(), F.col("id").desc(), F.col("play_id").desc())

base_plays = (
    df_plays
    .filter(F.col("device") == "phone")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "source",
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
    )
)

annotated = (
    base_plays
    .filter(F.col("track_id").isNotNull())
    .select(
        "*",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("played_at")).alias("days_since_played_at"),
        F.quarter(F.col("played_at")).alias("played_at_quarter"),
        F.date_format(F.col("played_at"), "yyyy-MM-dd").alias("played_at_label"),
    )
)

exploded_detail_plays = (
    annotated.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.is_skipped",
        "s.source",
        "s.is_skipped_label",
        "s.days_since_played_at",
        "s.played_at_label",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.device").alias("play_device"),
        F.col("p.listener_id").alias("play_listener_id"),
    )
)

without_matches = (
    exploded_detail_plays.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.listener_id") >= 13), "left_semi")
)

latest = (
    without_matches
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.max("played_at").alias("last_played_at"),
        F.sum("ms_played").alias("total_play_ms_played"),
        F.sum("ms_played").alias("total_play_ms_played_2"),
    )
)

with_stats = (
    latest.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.play_listener_id"), "left")
    .select(
        "s.*",
        "p.last_played_at",
        "p.total_play_ms_played",
        "p.total_play_ms_played_2",
    )
)

result = (
    with_stats
    .select(
        "id",
        "listener_id",
        "track_id",
        "is_skipped",
        "source",
        "is_skipped_label",
        "days_since_played_at",
        "played_at_label",
        "play_id",
        "play_ms_played",
        "play_played_at",
        "play_device",
        "play_listener_id",
        "last_played_at",
        "total_play_ms_played",
        "total_play_ms_played_2",
    )
)
