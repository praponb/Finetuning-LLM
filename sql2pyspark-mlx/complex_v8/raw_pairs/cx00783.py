from pyspark.sql import functions as F

recent_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.quarter(F.col("release_date")).alias("release_date_quarter"),
        (F.col("duration_sec") - F.col("bpm")).alias("duration_sec_minus_bpm"),
    )
)

filtered = (
    recent_tracks.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (~F.col("t.is_explicit")), "left_semi")
    .filter(F.col("bpm").between(82, 165))
)

result = (
    filtered
    .rollup("artist_id", "is_explicit", "release_date_quarter")
    .agg(F.sum("bpm").alias("total_bpm"))
    .orderBy(
        F.col("artist_id").asc_nulls_last(),
        F.col("is_explicit").asc_nulls_last(),
        F.col("release_date_quarter").asc_nulls_last(),
    )
)
