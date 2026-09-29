from pyspark.sql import functions as F

prepared = (
    df_tracks
    .filter((F.col("bpm") >= 94) | (F.col("duration_sec").between(165, 352)))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.least(F.col("bpm"), F.col("duration_sec")).alias("least_bpm_duration_sec"),
    )
)

with_parent_artist = (
    prepared.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.moods",
        "s.least_bpm_duration_sec",
        "a.artist_name",
        F.col("a.country").alias("artist_country"),
    )
)

tagged = (
    with_parent_artist
    .select(
        "*",
        (F.floor(F.col("least_bpm_duration_sec") / 50) * 50).alias("least_bpm_duration_sec_bucket"),
        (F.col("least_bpm_duration_sec") - F.col("duration_sec")).alias("least_bpm_duration_sec_minus_duration_sec"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.count("*").alias("track_count"))
)

summarized_rows = (
    tagged.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
    )
)

result = (
    summarized_rows
    .select(
        "id",
        F.col("duration_sec").alias("m_duration_sec"),
        F.col("least_bpm_duration_sec_minus_duration_sec").alias("m_least_bpm_duration_sec_minus_duration_sec"),
    )
    .unpivot(
        ids=["id"],
        values=["m_duration_sec", "m_least_bpm_duration_sec_minus_duration_sec"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
