from pyspark.sql import functions as F

recent = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("S%")) & (F.col("duration_sec").between(142, 352)))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.date_format(F.col("release_date"), "MMM yyyy").alias("release_date_label"),
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
    )
)

exploded_detail = (
    recent.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.is_explicit",
        "s.moods",
        "s.title_slug",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.device").alias("play_device"),
    )
)

result = (
    exploded_detail
    .cube("is_explicit", "artist_id", "play_device")
    .agg(F.min("play_ms_played").alias("min_play_ms_played"))
    .select(
        "is_explicit",
        "artist_id",
        F.coalesce(F.col("play_device"), F.lit("ALL")).alias("play_device"),
        "min_play_ms_played",
    )
    .orderBy(
        F.col("is_explicit").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
        F.col("play_device").asc_nulls_last(),
    )
)
