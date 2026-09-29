from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_month_artist_ge = Window.partitionBy(F.date_trunc("month", "release_date"), "artist_genre").orderBy("release_date")
w_artist_id_artist_genre = Window.partitionBy("artist_id", "artist_genre")
w_release_date_month_artist_ge_2 = Window.partitionBy(F.date_trunc("month", "release_date"), "artist_genre").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_release_date_month_artist_ge_3 = Window.partitionBy(F.date_trunc("month", "release_date"), "artist_genre").orderBy("release_date", "id")

cleaned_tracks = (
    df_tracks
    .filter(F.col("release_date") < F.lit("2025-03-08").cast("date"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

with_details_artist = (
    cleaned_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        "a.artist_name",
        F.col("a.genre").alias("artist_genre"),
    )
)

with_windows = (
    with_details_artist
    .select(
        "*",
        F.rank().over(w_release_date_month_artist_ge).alias("release_date_rank"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_id_artist_genre), F.lit(0)), 4).alias("bpm_share_of_artist_id_artist_genre"),
        F.count(F.lit(1)).over(w_release_date_month_artist_ge_2).alias("running_count"),
        F.row_number().over(w_release_date_month_artist_ge_3).alias("release_date_seq"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.max("played_at").alias("last_played_at"),
        F.max("played_at").alias("last_played_at_2"),
    )
)

summarized_rows = (
    with_windows.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select("s.*", "p.last_played_at", "p.last_played_at_2")
)

result = (
    summarized_rows
    .groupBy("last_played_at_2")
    .pivot("artist_genre", ["Indie", "Jazz", "Pop", "HipHop"])
    .agg(F.max("bpm_share_of_artist_id_artist_genre"))
    .orderBy("last_played_at_2")
)
