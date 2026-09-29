from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_track_bpm = Window.orderBy(F.col("track_bpm").desc(), "id")
w_listener_id_by_track_bpm_tie = Window.partitionBy("listener_id").orderBy(F.col("track_bpm").asc_nulls_last())
w_track_id_artist_genre = Window.partitionBy("track_id", "artist_genre")

filtered_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
    )
)

with_details_track = (
    filtered_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.is_skipped",
        "s.source",
        F.col("t.bpm").alias("track_bpm"),
        F.col("a.genre").alias("artist_genre"),
    )
)

with_windows = (
    with_details_track
    .select(
        "id",
        "listener_id",
        "source",
        F.round(F.percent_rank().over(w_all_by_track_bpm), 3).alias("track_bpm_percent_rank"),
        F.round(F.cume_dist().over(w_listener_id_by_track_bpm_tie), 3).alias("track_bpm_cume_dist"),
        F.max("track_bpm").over(w_track_id_artist_genre).alias("track_id_artist_genre_max_track_bpm"),
        (F.col("track_bpm") - F.lag("track_bpm").over(w_all_by_track_bpm)).alias("track_bpm_change"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "listener_id",
        "source",
        "track_bpm_percent_rank",
        "track_bpm_cume_dist",
        "track_id_artist_genre_max_track_bpm",
        "track_bpm_change",
        F.length(F.trim(F.col("source"))).alias("source_len"),
        F.coalesce(F.col("track_bpm_change"), F.lit(0)).alias("track_bpm_change_filled"),
    )
    .orderBy(F.col("listener_id").desc(), "id")
    .limit(10)
)
