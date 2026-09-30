from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_first_played_at = Window.partitionBy("artist_id").orderBy("first_played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id = Window.partitionBy("artist_id")
w_artist_id_by_first_played_at_2 = Window.partitionBy("artist_id").orderBy("first_played_at", "id").rowsBetween(-6, -1)

prepared = (
    df_tracks
    .filter(~F.array_contains(F.col("moods"), "sad"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.lower(F.trim(F.col("title"))).alias("title_clean"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.min("played_at").alias("first_played_at"))
)

profiled = (
    prepared.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select("s.*", "p.first_played_at")
)

annotated = (
    profiled
    .select(
        "id",
        "artist_id",
        "days_since_release_date",
        "title_clean",
        "first_played_at",
        F.lower(F.regexp_replace(F.trim(F.col("title_clean")), " ", "_")).alias("title_clean_slug"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("first_played_at")).alias("days_since_first_played_at"),
    )
)

with_trends = (
    annotated
    .select(
        "*",
        F.nth_value("first_played_at", 2).over(w_artist_id_by_first_played_at).alias("second_first_played_at"),
        F.max("days_since_first_played_at").over(w_artist_id).alias("artist_id_max_days_since_first_played_at"),
        F.round(F.avg("days_since_first_played_at").over(w_artist_id_by_first_played_at_2), 2).alias("days_since_first_played_at_avg_6_prior"),
        F.round(F.col("days_since_first_played_at") / F.nullif(F.sum("days_since_first_played_at").over(w_artist_id), F.lit(0)), 4).alias("days_since_first_played_at_share_of_artist_id"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "artist_id",
        "days_since_release_date",
        "first_played_at",
        "title_clean_slug",
        "days_since_first_played_at",
        "second_first_played_at",
        "artist_id_max_days_since_first_played_at",
        "days_since_first_played_at_avg_6_prior",
        F.when(F.col("days_since_release_date") >= 595, "A").when(F.col("days_since_release_date") >= 210, "B").otherwise("C").alias("days_since_release_date_tier"),
    )
    .orderBy(
        F.col("second_first_played_at").desc(),
        F.col("days_since_first_played_at").desc(),
    )
)
