from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_played_at_full = Window.orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source = Window.partitionBy("source")
w_all_by_played_at = Window.orderBy("played_at", "id")

scoped_plays = (
    df_plays
    .filter((F.col("listener_id") >= 9) & (~F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.date_trunc("day", F.col("played_at")).alias("played_at_day"),
    )
)

without_matches = (
    scoped_plays.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left_semi")
)

combined_details = (
    without_matches.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "left")
    .select(
        "s.*",
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.moods").alias("track_moods"),
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count(F.when(F.col("device") == "phone", 1)).alias("play_device_count"),
    )
)

with_stats = (
    combined_details.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_device_count"), F.lit(0)).alias("play_device_count"),
    )
)

with_windows = (
    with_stats
    .select(
        "*",
        F.nth_value("played_at_day", 2).over(w_all_by_played_at_full).alias("second_played_at_day"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_source), 2).alias("ms_played_vs_source_avg"),
        F.rank().over(w_all_by_played_at).alias("played_at_rank"),
        F.round(F.avg("ms_played").over(w_source), 2).alias("source_avg_ms_played"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "listener_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        "played_at_day",
        "track_release_date",
        "track_is_explicit",
        "artist_is_verified",
        "play_device_count",
        "second_played_at_day",
        "ms_played_vs_source_avg",
        "played_at_rank",
        "source_avg_ms_played",
    )
    .orderBy(F.col("track_release_date").asc(), "played_at_rank")
)
