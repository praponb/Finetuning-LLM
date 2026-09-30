from pyspark.sql import functions as F

prepared_tracks = (
    df_tracks
    .filter((F.col("duration_sec") <= 402) & (F.col("bpm") >= 104))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        F.concat(F.col("title"), F.lit("#"), F.col("id").cast("string")).alias("title_ref"),
        F.date_format(F.col("release_date"), "MMM yyyy").alias("release_date_label"),
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
    )
)

without_matches = (
    prepared_tracks.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.array_contains(F.col("t.moods"), "sad")), "left_anti")
)

without_matches_2 = (
    without_matches.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.array_contains(F.col("t.moods"), "sad")) & (F.col("t.release_date").between(F.lit("2024-06-03").cast("date"), F.lit("2025-11-14").cast("date"))), "left_anti")
)

result = (
    without_matches_2
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        "title_ref",
        "release_date_label",
        "duration_sec_bucket",
        F.lower(F.regexp_replace(F.trim(F.col("title_ref")), " ", "_")).alias("title_ref_slug"),
    )
    .orderBy(F.col("duration_sec_bucket").asc(), F.col("duration_sec").asc())
)
