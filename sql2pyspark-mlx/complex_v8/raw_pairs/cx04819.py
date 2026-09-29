from pyspark.sql import functions as F

valid = (
    df_tracks
    .filter(F.col("artist_id") <= 10)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.abs(F.col("duration_sec") - 183).alias("duration_sec_distance"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("release_date")).alias("days_since_release_date"),
    )
)

enriched = (
    valid
    .filter(F.col("days_since_release_date") <= 490)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "days_since_release_date",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("release_date")).alias("days_since_release_date_2"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count(F.when(F.col("source").isin("radio", "playlist"), 1)).alias("play_source_count"),
        F.count("*").alias("play_count"),
        F.min("played_at").alias("first_played_at"),
    )
)

summarized_rows = (
    enriched.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_source_count"), F.lit(0)).alias("play_source_count"),
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.first_played_at",
    )
)

derived = (
    summarized_rows
    .select(
        "*",
        F.year(F.col("first_played_at")).alias("first_played_at_year"),
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
        F.to_date(F.col("first_played_at")).alias("first_played_at_date"),
    )
)

result = (
    derived
    .select(
        "id",
        "artist_id",
        "days_since_release_date",
        "days_since_release_date_2",
        "play_source_count",
        "play_count",
        "first_played_at_year",
        "duration_sec_bucket",
        "first_played_at_date",
        F.add_months(F.col("first_played_at_date"), 1).alias("first_played_at_date_plus_1m"),
        (F.col("duration_sec") - F.col("first_played_at_year")).alias("duration_sec_minus_first_played_at_year"),
    )
)
