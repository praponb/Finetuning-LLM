from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id = Window.partitionBy("artist_id")
w_artist_id_by_bpm_running = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_tracks
    .filter(F.col("title").isNotNull())
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
    )
)

with_windows = (
    valid
    .select(
        "id",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        F.max("duration_sec").over(w_artist_id).alias("artist_id_max_duration_sec"),
        F.max("bpm").over(w_artist_id_by_bpm_running).alias("max_bpm_so_far"),
    )
)

line_level = (
    with_windows.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.ms_played") >= 73625), "inner")
    .select(
        "s.id",
        "s.duration_sec",
        "s.moods",
        "s.artist_id_max_duration_sec",
        "s.max_bpm_so_far",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.listener_id").alias("play_listener_id"),
    )
)

result = (
    line_level
    .groupingSets(
        [["play_listener_id", "play_id"], ["play_listener_id"], []],
        "play_listener_id", "play_id",
    )
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.count("*").alias("cnt"),
    )
)
