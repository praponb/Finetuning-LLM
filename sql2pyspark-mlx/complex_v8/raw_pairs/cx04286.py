from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_bpm = Window.orderBy(F.col("bpm").asc_nulls_last(), "id")
w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_all_by_bpm_full = Window.orderBy(F.col("bpm").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_tracks
    .filter((F.col("bpm") > 85) | (F.col("artist_id").isNotNull()))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        F.date_format(F.col("release_date"), "MMM yyyy").alias("release_date_label"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.row_number().over(w_all_by_bpm).alias("bpm_seq"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_id_is_explicit), F.lit(0)), 4).alias("bpm_share_of_artist_id_is_explicit"),
        F.last("release_date").over(w_all_by_bpm_full).alias("last_release_date"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.max("ms_played").alias("max_play_ms_played"),
        F.max("ms_played").alias("max_play_ms_played_2"),
        F.count("*").alias("play_count"),
    )
)

with_stats = (
    with_windows.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "p.max_play_ms_played",
        "p.max_play_ms_played_2",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

screened = (
    with_stats.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left_semi")
)

result = (
    screened
    .filter(F.col("max_play_ms_played_2") >= 123500)
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "release_date_label",
        "bpm_seq",
        "bpm_share_of_artist_id_is_explicit",
        "max_play_ms_played",
        "max_play_ms_played_2",
        "play_count",
    )
)
