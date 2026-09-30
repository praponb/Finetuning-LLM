from pyspark.sql import functions as F

valid = (
    df_tracks
    .filter(F.col("artist_id") >= 5)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
    )
)

without_matches = (
    valid.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (~F.col("a.genre").isin("Jazz", "Indie")) & (F.col("a.country") == "TH"), "left_anti")
)

result = (
    without_matches
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "bpm",
    )
    .orderBy("duration_sec", "id")
    .limit(10)
)
