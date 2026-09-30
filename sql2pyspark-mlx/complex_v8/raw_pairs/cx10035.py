from pyspark.sql import functions as F

valid = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix"),
        (F.col("bpm") + F.col("duration_sec")).alias("bpm_plus_duration_sec"),
    )
)

enriched = (
    valid
    .filter(F.lower(F.col("title")).like("%ur%"))
    .withColumn("bpm_plus_duration_sec_distance", F.abs(F.col("bpm_plus_duration_sec") - 357))
)

with_parent_artist = (
    enriched.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.country").isin("BR", "JP", "TH"))
    .select("s.*", F.col("a.country").alias("artist_country"))
)

with_activity = (
    with_parent_artist.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.country").isin("JP", "TH")), "left_semi")
)

result = (
    with_activity
    .filter(F.col("bpm_plus_duration_sec_distance").between(180, 510))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        "bpm_bucket",
        "title_prefix",
        "bpm_plus_duration_sec",
        "artist_country",
        (F.floor(F.col("bpm_plus_duration_sec") / 100) * 100).alias("bpm_plus_duration_sec_bucket"),
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
    )
    .orderBy(F.col("release_date").asc(), "id")
    .limit(20)
)
