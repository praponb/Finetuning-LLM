from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_is_explicit_by_fir = Window.partitionBy("artist_id", "is_explicit").orderBy("first_played_at_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_country_artist_id = Window.partitionBy("artist_country", "artist_id")
w_artist_id_is_explicit_by_fir_2 = Window.partitionBy("artist_id", "is_explicit").orderBy("first_played_at_2", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_tracks = (
    df_tracks
    .filter((F.col("duration_sec").between(128, 352)) | (~F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.coalesce(F.col("bpm"), F.lit(0)).alias("bpm_filled"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.min("played_at").alias("first_played_at_2"),
    )
)

summarized_rows = (
    filtered_tracks.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select("s.*", "p.first_played_at", "p.first_played_at_2")
)

with_details_artist = (
    summarized_rows.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.*",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.country").alias("artist_country"),
        "a.artist_name",
    )
)

screened = (
    with_details_artist.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.device") == "desktop") & (F.col("p.source").isNotNull()), "left_anti")
)

with_trends = (
    screened
    .select(
        "*",
        F.first("release_date", ignorenulls=True).over(w_artist_id_is_explicit_by_fir).alias("first_known_release_date"),
        F.round(F.avg("bpm_filled").over(w_artist_country_artist_id), 2).alias("artist_country_artist_id_avg_bpm_filled"),
        F.nth_value("first_played_at_2", 2).over(w_artist_id_is_explicit_by_fir_2).alias("second_first_played_at_2"),
    )
)

screened_2 = (
    with_trends.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.upper(F.trim(F.col("t.title"))).like("G%")), "left_anti")
)

result = (
    screened_2
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "bpm",
        "artist_debut_year",
        "artist_country",
        "artist_name",
        "first_known_release_date",
        "artist_country_artist_id_avg_bpm_filled",
        "second_first_played_at_2",
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
    )
    .orderBy(F.col("first_known_release_date").asc())
)
