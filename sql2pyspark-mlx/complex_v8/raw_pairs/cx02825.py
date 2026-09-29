from pyspark.sql import functions as F

recent = (
    df_tracks
    .filter(F.upper(F.trim(F.col("title"))).like("G%"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "moods",
        F.date_trunc("month", F.col("release_date")).alias("release_date_month"),
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
    )
)

screened = (
    recent.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.is_skipped")), "left_anti")
    .filter(F.col("artist_id").isNotNull())
)

tagged = (
    screened
    .select(
        "id",
        "duration_sec",
        "release_date",
        "moods",
        "release_date_month",
        "duration_sec_bucket",
        F.abs(F.col("duration_sec") - 209).alias("duration_sec_distance"),
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix"),
    )
)

result = (
    tagged
    .filter(F.array_contains(F.col("moods"), "sad"))
    .select(
        "id",
        "moods",
        "duration_sec_distance",
        "title_prefix",
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket_2"),
    )
    .orderBy("duration_sec_distance", F.col("id").asc_nulls_last())
)
