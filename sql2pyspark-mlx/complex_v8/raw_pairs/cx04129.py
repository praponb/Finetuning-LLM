from pyspark.sql import functions as F

prepared = (
    df_tracks
    .filter((F.col("duration_sec") >= 154) & (F.length(F.trim(F.col("title"))) >= 6))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label"),
        F.substring(F.trim(F.col("title")), 1, 3).alias("title_prefix"),
    )
)

with_details = (
    prepared.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.debut_year") > 1997)
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.release_date_label",
        F.col("a.country").alias("artist_country"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

detail_rows = (
    with_details.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.release_date_label",
        "s.artist_country",
        F.col("p.id").alias("play_id"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

annotated = (
    detail_rows
    .filter(F.col("artist_id") >= 9)
    .select(
        "*",
        F.when(F.col("duration_sec") >= 352, "HIGH").when(F.col("duration_sec") >= 164, "MEDIUM").otherwise("LOW").alias("duration_sec_tier"),
    )
)

result = (
    annotated
    .select(
        "id",
        "artist_id",
        "release_date",
        "release_date_label",
        "artist_country",
        "play_id",
        "play_listener_id",
        "play_is_skipped",
        "duration_sec_tier",
        F.date_format(F.col("release_date"), "yyyy-MM").alias("release_date_label_2"),
        F.when(F.col("play_is_skipped"), "yes").otherwise("no").alias("play_is_skipped_label"),
    )
    .orderBy(F.col("release_date_label").asc())
)
