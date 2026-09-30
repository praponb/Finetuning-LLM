from pyspark.sql import functions as F

scoped_tracks = (
    df_tracks
    .filter((F.size(F.col("moods")) >= 2) & (F.col("artist_id").isNotNull()))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
        F.date_trunc("month", F.col("release_date")).alias("release_date_month"),
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
    )
)

labeled = (
    scoped_tracks
    .filter(F.col("is_explicit_flag") > 0)
    .select(
        "*",
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket_2"),
        F.abs(F.col("duration_sec_bucket") - 288).alias("duration_sec_bucket_distance"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.min("played_at").alias("first_played_at"))
)

summarized_rows = (
    labeled.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select("s.*", "p.first_played_at")
)

result = (
    summarized_rows
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        "is_explicit_flag",
        "release_date_month",
        "first_played_at",
        F.array_join(F.array_sort(F.col("moods")), ",").alias("moods_text"),
        F.round(F.col("duration_sec") / F.nullif(F.col("duration_sec_bucket"), F.lit(0)), 3).alias("duration_sec_per_duration_sec_bucket"),
    )
    .orderBy(F.col("moods_text").asc_nulls_last(), "is_explicit_flag")
)
