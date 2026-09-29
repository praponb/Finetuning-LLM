from pyspark.sql import functions as F

scoped_plays = (
    df_plays
    .filter((F.to_date(F.col("played_at")) < F.lit("2025-02-12").cast("date")) & (~F.col("device").isin("speaker", "car")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
    )
)

with_details_track = (
    scoped_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .filter(F.col("t.duration_sec") >= 142)
    .select(
        "s.*",
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.title").alias("track_title"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.sum("ms_played").alias("total_play_ms_played"),
    )
)

summarized_rows = (
    with_details_track.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select("s.*", "p.first_played_at", "p.total_play_ms_played")
)

without_matches = (
    summarized_rows
    .crossJoin(summarized_rows.agg(F.avg("total_play_ms_played").alias("overall_avg_total_play_ms_played")))
    .filter(F.col("total_play_ms_played") > F.col("overall_avg_total_play_ms_played"))
    .drop("overall_avg_total_play_ms_played")
)

result = (
    without_matches
    .groupingSets(
        [["is_skipped", "source", "listener_id"], ["is_skipped"], ["source"], []],
        "is_skipped", "source", "listener_id",
    )
    .agg(
        F.sum("total_play_ms_played").alias("total_total_play_ms_played"),
        F.max("track_duration_sec").alias("max_track_duration_sec"),
    )
)
