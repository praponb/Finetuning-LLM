from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_artist_genre = Window.partitionBy("is_explicit", "artist_genre")
w_track_count_by_track_count = Window.partitionBy("track_count").orderBy("track_count", "id")

base_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.abs(F.col("duration_sec") - 239).alias("duration_sec_distance"),
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.count("*").alias("track_count"),
        F.count("*").alias("track_count_2"),
        F.min("release_date").alias("first_track_release_date"),
    )
)

with_stats = (
    base_tracks.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
        F.coalesce(F.col("t.track_count_2"), F.lit(0)).alias("track_count_2"),
        "t.first_track_release_date",
    )
)

with_parent = (
    with_stats.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.debut_year") <= 2006), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.is_explicit",
        "s.days_since_release_date",
        "s.duration_sec_distance",
        "s.track_count",
        "s.track_count_2",
        "s.first_track_release_date",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.genre").alias("artist_genre"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

sequenced = (
    with_parent
    .withColumn("duration_sec_distance_share_of_is_explicit_artis", F.round(F.col("duration_sec_distance") / F.nullif(F.sum("duration_sec_distance").over(w_is_explicit_artist_genre), F.lit(0)), 4))
    .withColumn("track_count_seq", F.row_number().over(w_track_count_by_track_count))
)

result = (
    sequenced
    .groupBy("track_count_2", "is_explicit")
    .pivot("artist_genre", ["HipHop", "Pop"])
    .agg(F.count("track_count_seq"))
    .orderBy("track_count_2", "is_explicit")
)
