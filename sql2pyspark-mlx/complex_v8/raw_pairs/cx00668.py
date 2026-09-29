from pyspark.sql import functions as F

prepared_tracks = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("G%")) & (F.year(F.col("release_date")) == 2025))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
        F.lower(F.trim(F.col("title"))).alias("title_clean"),
        (F.col("bpm") - F.col("duration_sec")).alias("bpm_minus_duration_sec"),
    )
)

filtered = (
    prepared_tracks.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.genre") != "HipHop") & (~F.col("a.is_verified")), "left_semi")
)

annotated = filtered.withColumn("title_slug_len", F.length(F.trim(F.col("title_slug"))))

labeled = (
    annotated
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "moods",
        "title_slug",
        "title_clean",
        "bpm_minus_duration_sec",
        "title_slug_len",
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label"),
        F.round(F.col("title_slug_len") / F.nullif(F.col("bpm"), F.lit(0)), 3).alias("title_slug_len_per_bpm"),
    )
)

result = (
    labeled
    .select(
        "id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm_minus_duration_sec",
        "title_slug_len",
        (F.col("bpm_minus_duration_sec") - F.col("title_slug_len")).alias("bpm_minus_duration_sec_minus_title_slug_len"),
    )
    .orderBy(F.col("id").desc(), F.col("bpm_minus_duration_sec").desc())
)
