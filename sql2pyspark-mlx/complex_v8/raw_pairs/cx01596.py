from pyspark.sql import functions as F

valid_tracks = (
    df_tracks
    .filter(F.lower(F.col("title")).like("%li%"))
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

without_matches = (
    valid_tracks.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.size(F.col("t.moods")) >= 1) & (~F.col("t.is_explicit")), "left_anti")
)

derived = (
    without_matches
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.date_trunc("year", F.col("release_date")).alias("release_date_year"),
    )
)

filtered = (
    derived.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left_semi")
    .filter(F.col("is_explicit"))
)

tagged = (
    filtered
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "bpm",
        (F.col("release_date_year") + F.expr("INTERVAL 30 MINUTES")).alias("release_date_year_plus_30m"),
        (F.col("bpm") + F.col("duration_sec")).alias("bpm_plus_duration_sec"),
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
    )
)

result = (
    tagged
    .filter(F.col("release_date").between(F.lit("2024-04-29").cast("date"), F.lit("2025-08-02").cast("date")))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "release_date_year_plus_30m",
        "bpm_plus_duration_sec",
        F.when(F.col("bpm") >= 156, "HIGH").when(F.col("bpm") >= 96, "MEDIUM").otherwise("LOW").alias("bpm_tier"),
    )
    .orderBy(F.col("id").desc(), F.col("bpm_plus_duration_sec").desc_nulls_first())
)
