from pyspark.sql import functions as F

recent = (
    df_tracks
    .filter((F.col("release_date") >= F.lit("2024-11-14").cast("date")) & (F.upper(F.trim(F.col("title"))).like("E%")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
    )
)

with_parent_artist = (
    recent.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.debut_year") >= 1997), "inner")
    .filter(~F.col("a.is_verified"))
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.is_explicit",
        "s.bpm",
        F.col("a.country").alias("artist_country"),
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

result = (
    with_parent_artist
    .select(
        "id",
        F.col("duration_sec").alias("duration_sec_v"),
        F.col("artist_debut_year").alias("artist_debut_year_v"),
    )
    .unpivot(
        ids=["id"],
        values=["duration_sec_v", "artist_debut_year_v"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "field")
)
