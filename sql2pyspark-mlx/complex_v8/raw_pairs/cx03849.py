from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_listener_id = Window.partitionBy("track_id", "listener_id")

base_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
    )
)

with_details = (
    base_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        F.col("l.interests").alias("listener_interests"),
        F.col("t.title").alias("track_title"),
        F.col("t.release_date").alias("track_release_date"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played"),
        F.max("ms_played").alias("max_play_ms_played"),
    )
)

with_stats = (
    with_details.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select("s.*", "p.avg_play_ms_played", "p.max_play_ms_played")
)

joined = (
    with_stats.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .filter(F.col("t.artist_id").isNotNull())
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.listener_interests",
        "s.track_title",
        "s.track_release_date",
        "s.avg_play_ms_played",
        "s.max_play_ms_played",
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("t.bpm").alias("track_bpm"),
    )
)

ranked = (
    joined
    .select(
        "id",
        "listener_id",
        "device",
        "listener_interests",
        "track_title",
        "track_release_date",
        "avg_play_ms_played",
        "max_play_ms_played",
        "track_artist_id",
        F.round(F.avg("max_play_ms_played").over(w_track_id_listener_id), 2).alias("track_id_listener_id_avg_max_play_ms_played"),
        F.max("avg_play_ms_played").over(w_track_id_listener_id).alias("track_id_listener_id_max_avg_play_ms_played"),
    )
)

without_matches = (
    ranked
    .crossJoin(ranked.agg(F.avg("avg_play_ms_played").alias("overall_avg_avg_play_ms_played")))
    .filter(F.col("avg_play_ms_played") > F.col("overall_avg_avg_play_ms_played"))
    .drop("overall_avg_avg_play_ms_played")
)

result = (
    without_matches
    .select(
        "id",
        "track_id_listener_id_max_avg_play_ms_played",
        "track_id_listener_id_avg_max_play_ms_played",
        "avg_play_ms_played",
    )
    .unpivot(
        ids=["id"],
        values=["track_id_listener_id_max_avg_play_ms_played", "track_id_listener_id_avg_max_play_ms_played", "avg_play_ms_played"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
