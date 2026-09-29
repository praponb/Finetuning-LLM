from pyspark.sql import functions as F

prepared = (
    df_tracks
    .filter(F.col("is_explicit"))
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

track_side = prepared.select(F.col("id").alias("ref_id"), "title")

artist_side = (
    df_artists
    .filter(F.col("debut_year") < 2016)
    .select(F.col("id").alias("ref_id"), F.col("artist_name").alias("title"))
)

result = (
    track_side
    .unionByName(artist_side)
    .distinct()
    .orderBy("title", "ref_id")
)
