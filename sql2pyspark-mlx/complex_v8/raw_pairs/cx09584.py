from pyspark.sql import functions as F

prepared = (
    df_tracks
    .filter((F.year(F.col("release_date")) == 2024) & (F.length(F.trim(F.col("title"))) >= 8))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
    )
)

enriched_artist = (
    prepared.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.artist_name").isNotNull())
    .select(
        "s.*",
        F.col("a.is_verified").alias("artist_is_verified"),
        "a.artist_name",
    )
)

result = (
    enriched_artist
    .select(
        "id",
        "bpm_bucket",
        "duration_sec",
        "bpm",
    )
    .unpivot(
        ids=["id"],
        values=["bpm_bucket", "duration_sec", "bpm"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "field")
)
