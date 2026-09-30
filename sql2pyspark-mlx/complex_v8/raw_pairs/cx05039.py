from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_is_explicit_by_bpm = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("bpm").asc_nulls_last(), "id")
w_release_date_month_is_explic = Window.partitionBy(F.month("release_date"), "is_explicit")
w_artist_id_is_explicit_by_bpm_2 = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("bpm").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("C%")) & (F.col("release_date") < F.lit("2025-03-08").cast("date")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.concat(F.col("title"), F.lit("#"), F.col("duration_sec").cast("string")).alias("title_ref"),
    )
)

windowed = (
    base
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        "days_since_release_date",
        F.row_number().over(w_artist_id_is_explicit_by_bpm).alias("rn"),
        F.min("duration_sec").over(w_release_date_month_is_explic).alias("release_date_month_is_explicit_min_duration_sec"),
        F.nth_value("days_since_release_date", 2).over(w_artist_id_is_explicit_by_bpm_2).alias("second_days_since_release_date"),
        (F.col("days_since_release_date") - F.lag("days_since_release_date").over(w_artist_id_is_explicit_by_bpm)).alias("days_since_release_date_change"),
        F.round(F.col("days_since_release_date") - F.avg("days_since_release_date").over(w_release_date_month_is_explic), 2).alias("days_since_release_date_vs_release_date_month_is"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played"),
        F.count("*").alias("play_count"),
        F.min("ms_played").alias("min_play_ms_played"),
    )
)

with_counts = (
    windowed.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "p.avg_play_ms_played",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.min_play_ms_played",
    )
)

with_activity = (
    with_counts.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (~F.col("p.device").isin("tv", "speaker")), "left_semi")
)

result = (
    with_activity
    .rollup("rn", "play_count", "is_explicit")
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("artist_id").alias("distinct_artist_ids"),
    )
    .orderBy(
        F.col("rn").asc_nulls_last(),
        F.col("play_count").asc_nulls_last(),
        F.col("is_explicit").asc_nulls_last(),
    )
)
