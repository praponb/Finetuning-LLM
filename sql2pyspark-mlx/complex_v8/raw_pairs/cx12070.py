from pyspark.sql import functions as F

valid = (
    df_tracks
    .filter((~F.array_contains(F.col("moods"), "chill")) & (F.col("duration_sec") >= 142))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.month(F.col("release_date")).alias("release_date_month"),
    )
)

joined = (
    valid.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        "s.release_date_month",
        F.col("a.country").alias("artist_country"),
        F.col("a.is_verified").alias("artist_is_verified"),
        "a.artist_name",
    )
)

result = (
    joined
    .groupBy("is_explicit", "artist_id")
    .pivot("artist_country", ["JP", "US", "KR", "BR"])
    .agg(F.avg("release_date_month"))
)
