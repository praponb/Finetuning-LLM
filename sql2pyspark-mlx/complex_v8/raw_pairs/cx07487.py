from pyspark.sql import functions as F

prepared = (
    df_tracks
    .filter(F.col("bpm") < 142)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
    )
)

enriched = (
    prepared
    .filter(F.col("release_date") < F.lit("2025-08-02").cast("date"))
    .select(
        "*",
        F.dayofweek(F.col("release_date")).alias("release_date_dayofweek"),
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
    )
)

with_details_artist = (
    enriched.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(~F.col("a.is_verified"))
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.duration_sec_filled",
        F.col("a.country").alias("artist_country"),
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

result = (
    with_details_artist
    .select(
        "id",
        F.col("duration_sec").alias("m_duration_sec"),
        F.col("artist_debut_year").alias("m_artist_debut_year"),
    )
    .unpivot(
        ids=["id"],
        values=["m_duration_sec", "m_artist_debut_year"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
