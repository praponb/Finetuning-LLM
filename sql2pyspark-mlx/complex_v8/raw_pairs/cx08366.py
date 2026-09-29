from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_is_verified_track_id = Window.partitionBy("artist_is_verified", "track_id")
w_track_id_is_skipped_by_playe = Window.partitionBy("track_id", "is_skipped").orderBy("played_at", "id")
w_track_id_played_at_plus_1d_m = Window.partitionBy("track_id", F.date_trunc("month", "played_at_plus_1d")).orderBy(F.col("track_bpm").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id_played_at_plus_1d_m_2 = Window.partitionBy("track_id", F.date_trunc("month", "played_at_plus_1d")).orderBy(F.col("track_bpm").asc_nulls_last())

recent_plays = (
    df_plays
    .filter((F.col("source") == "radio") & (F.col("device").isin("speaker", "phone")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.when(F.col("source").isin("search", "radio"), "group_a").when(F.col("source") == "album", "group_b").otherwise("other").alias("source_group"),
        (F.col("played_at") + F.expr("INTERVAL 1 DAYS")).alias("played_at_plus_1d"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played"),
        F.sum("ms_played").alias("total_play_ms_played"),
    )
)

with_stats = (
    recent_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        "p.first_played_at",
        "p.avg_play_ms_played",
        "p.total_play_ms_played",
    )
)

play_stats_2 = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.max("played_at").alias("last_played_at"),
        F.max("ms_played").alias("max_play_ms_played"),
    )
)

with_stats_2 = (
    with_stats.alias("s")
    .join(play_stats_2.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select("s.*", "p.last_played_at", "p.max_play_ms_played")
)

screened = (
    with_stats_2.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.track_id").isNotNull()), "left_anti")
)

combined_details = (
    screened.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.is_skipped",
        "s.source",
        "s.source_group",
        "s.played_at_plus_1d",
        "s.first_played_at",
        "s.max_play_ms_played",
        F.col("t.bpm").alias("track_bpm"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

windowed = (
    combined_details
    .select(
        "*",
        F.max("track_bpm").over(w_artist_is_verified_track_id).alias("artist_is_verified_track_id_max_track_bpm"),
        F.lead("played_at").over(w_track_id_is_skipped_by_playe).alias("next_played_at"),
        F.nth_value("played_at", 2).over(w_track_id_played_at_plus_1d_m).alias("second_played_at"),
        F.round(F.percent_rank().over(w_track_id_played_at_plus_1d_m_2), 3).alias("track_bpm_percent_rank"),
    )
)

result = (
    windowed
    .filter(F.col("listener_id") >= 18)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
        "played_at_plus_1d",
        "first_played_at",
        "max_play_ms_played",
        "track_bpm",
        "artist_is_verified",
        "artist_is_verified_track_id_max_track_bpm",
        "next_played_at",
        "track_bpm_percent_rank",
    )
    .orderBy(F.col("artist_is_verified_track_id_max_track_bpm").desc())
)
