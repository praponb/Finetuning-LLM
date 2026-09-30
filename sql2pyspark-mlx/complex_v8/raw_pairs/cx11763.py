from pyspark.sql import functions as F

scoped = (
    df_tracks
    .filter((~F.array_contains(F.col("moods"), "focus")) | (F.col("bpm") < 155))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        "moods",
    )
)

enriched = (
    scoped
    .filter(F.col("artist_id").isNotNull())
    .withColumn("duration_sec_per_bpm", F.round(F.col("duration_sec") / F.nullif(F.col("bpm"), F.lit(0)), 3))
)

scored = (
    enriched
    .filter(F.col("duration_sec") > 184)
    .select(
        "*",
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix"),
        F.upper(F.trim(F.col("title"))).alias("title_clean"),
    )
)

with_details_artist = (
    scored.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.bpm",
        "s.moods",
        "s.duration_sec_per_bpm",
        F.col("a.country").alias("artist_country"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

flattened = (
    with_details_artist
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        "duration_sec_per_bpm",
        "artist_country",
        "artist_is_verified",
        F.explode_outer("moods").alias("mood"),
    )
)

result = (
    flattened
    .groupBy("mood")
    .pivot("artist_country", ["KR", "JP", "TH"])
    .agg(F.avg("duration_sec_per_bpm"))
)
