from pyspark.sql import functions as F

recent = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("S%")) & (F.col("release_date").between(F.lit("2024-04-21").cast("date"), F.lit("2025-11-29").cast("date"))))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.last_day(F.col("release_date")).alias("release_date_month_end"),
    )
)

joined = (
    recent.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.moods",
        "s.release_date_month_end",
        "a.artist_name",
    )
)

result = (
    joined
    .filter(F.col("artist_id").isNotNull())
    .select(
        "id",
        "artist_id",
        "moods",
        "release_date_month_end",
        "artist_name",
    )
)
