from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_play_id_by_durat = Window.partitionBy("is_explicit", "play_id").orderBy("duration_sec_filled", "id", "play_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_by_artist_debut = Window.partitionBy("is_explicit").orderBy("artist_debut_year")
w_is_explicit_play_id_by_durat_2 = Window.partitionBy("is_explicit", "play_id").orderBy("duration_sec_filled", "id", "play_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_by_artist_debut_2 = Window.partitionBy("is_explicit").orderBy("artist_debut_year", "id", "play_id")

cleaned_tracks = (
    df_tracks
    .filter(F.year(F.col("release_date")) == 2024)
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "bpm",
        "moods",
        (F.col("bpm") + F.col("duration_sec")).alias("bpm_plus_duration_sec"),
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
        F.array_contains(F.col("moods"), "party").alias("has_party"),
    )
)

with_children = (
    cleaned_tracks.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        "s.duration_sec_filled",
        "s.has_party",
        F.col("p.id").alias("play_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.source").alias("play_source"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.device").alias("play_device"),
    )
)

with_details = (
    with_children.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.genre") != "Classical")
    .select(
        "s.*",
        F.col("a.genre").alias("artist_genre"),
        "a.artist_name",
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

windowed = (
    with_details
    .select(
        "*",
        F.last("play_played_at", ignorenulls=True).over(w_is_explicit_play_id_by_durat).alias("last_known_play_played_at"),
        F.dense_rank().over(w_is_explicit_by_artist_debut).alias("artist_debut_year_dense_rank"),
        F.first("play_played_at").over(w_is_explicit_play_id_by_durat_2).alias("first_play_played_at"),
        F.round(F.percent_rank().over(w_is_explicit_by_artist_debut_2), 3).alias("artist_debut_year_percent_rank"),
        F.ntile(10).over(w_is_explicit_by_artist_debut_2).alias("artist_debut_year_decile"),
    )
)

result = (
    windowed
    .groupingSets(
        [["play_id", "artist_debut_year_decile", "play_is_skipped"], ["play_id"], []],
        "play_id", "artist_debut_year_decile", "play_is_skipped",
    )
    .agg(
        F.grouping("play_id").alias("is_total_play_id"),
        F.count(F.when(F.upper(F.trim(F.col("title"))).like("C%"), 1)).alias("n_upper_trim_title_like_c"),
    )
)
