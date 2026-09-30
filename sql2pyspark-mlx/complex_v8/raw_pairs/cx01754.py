from pyspark.sql import functions as F

scoped_tracks = (
    df_tracks
    .filter(~F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.round(F.col("bpm") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("bpm_per_duration_sec"),
        F.date_trunc("year", F.col("release_date")).alias("release_date_year"),
    )
)

combined_details = (
    scoped_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.lower(F.col("a.artist_name")).like("%pe%"))
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.bpm",
        "s.release_date_year",
        "a.artist_name",
    )
)

labeled = (
    combined_details
    .filter(F.col("artist_id").isNotNull())
    .select(
        "*",
        F.length(F.trim(F.col("title"))).alias("title_len"),
        F.concat_ws("-", F.col("title"), F.col("artist_name")).alias("title_artist_name_label"),
        F.month(F.col("release_date_year")).alias("release_date_year_month"),
    )
)

filtered = (
    labeled.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left_semi")
    .filter(F.col("title_len") > 3)
)

screened = (
    filtered.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left_semi")
)

with_parent = (
    screened.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.bpm",
        "s.release_date_year",
        "s.title_artist_name_label",
        "s.release_date_year_month",
        F.col("a.artist_name").alias("artist_artist_name"),
    )
)

without_matches = (
    with_parent.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.is_explicit")) & (F.col("t.release_date").between(F.lit("2024-09-26").cast("date"), F.lit("2025-04-02").cast("date"))), "left_anti")
)

result = (
    without_matches
    .filter(F.col("bpm") >= 75)
    .select(
        "id",
        "artist_id",
        "bpm",
        "title_artist_name_label",
        "artist_artist_name",
    )
    .orderBy(F.col("artist_id").desc(), "title_artist_name_label", "id")
    .limit(10)
)
