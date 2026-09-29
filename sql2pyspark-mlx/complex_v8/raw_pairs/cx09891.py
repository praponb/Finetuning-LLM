from pyspark.sql import functions as F

scoped = (
    df_tracks
    .filter(F.col("release_date") < F.lit("2024-12-03").cast("date"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        "moods",
        F.when(F.col("duration_sec") >= 312, "A").when(F.col("duration_sec") >= 154, "B").otherwise("C").alias("duration_sec_tier"),
    )
)

scored = (
    scoped
    .filter(F.array_contains(F.col("moods"), "sad"))
    .select(
        "*",
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
        F.when(F.col("bpm") >= 163, "A").when(F.col("bpm") >= 94, "B").otherwise("C").alias("bpm_tier"),
    )
)

with_parent_artist = (
    scored.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.is_verified")), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.bpm",
        "s.moods",
        "s.duration_sec_tier",
        "s.bpm_tier",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.country").alias("artist_country"),
        "a.artist_name",
    )
)

with_parent = (
    with_parent_artist.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.moods",
        "s.duration_sec_tier",
        "s.bpm_tier",
        "s.artist_genre",
        "s.artist_country",
        "s.artist_name",
        F.col("a.artist_name").alias("artist_artist_name"),
        F.col("a.country").alias("artist_country_2"),
        F.col("a.genre").alias("artist_genre_2"),
    )
)

enriched_artist = (
    with_parent.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.moods",
        "s.duration_sec_tier",
        "s.bpm_tier",
        "s.artist_genre",
        "s.artist_country",
        "s.artist_name",
        "s.artist_artist_name",
        "s.artist_country_2",
        F.col("a.country").alias("artist_country_3"),
        F.col("a.genre").alias("artist_genre_3"),
        F.col("a.artist_name").alias("artist_artist_name_2"),
    )
)

result = (
    enriched_artist
    .cube("artist_genre_3", "artist_id")
    .agg(
        F.round(100 * F.count(F.when(F.upper(F.trim(F.col("artist_artist_name"))).like("S%"), 1)) / F.count("*"), 1).alias("pct_upper_trim_artist_artist_nam"),
        F.array_sort(F.collect_set("artist_country_2")).alias("artist_country_2_list"),
    )
)
