from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moods_count = Window.partitionBy("moods_count")
w_artist_id_by_days_since_rele = Window.partitionBy("artist_id").orderBy("days_since_release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_tracks
    .filter((F.col("artist_id").isNotNull()) | (F.col("release_date").between(F.lit("2024-09-26").cast("date"), F.lit("2025-11-29").cast("date"))))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        F.date_trunc("quarter", F.col("release_date")).alias("release_date_quarter"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.round(F.col("bpm") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("bpm_per_duration_sec"),
    )
)

scored = (
    valid
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        "release_date_quarter",
        "days_since_release_date",
        "bpm_per_duration_sec",
        F.concat(F.col("title"), F.lit("#"), F.col("artist_id").cast("string")).alias("title_ref"),
        F.size(F.col("moods")).alias("moods_count"),
    )
)

with_trends = (
    scored
    .select(
        "*",
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_moods_count), F.lit(0)), 4).alias("duration_sec_share_of_moods_count"),
        F.round(F.col("days_since_release_date") / F.nullif(F.sum("days_since_release_date").over(w_moods_count), F.lit(0)), 4).alias("days_since_release_date_share_of_moods_count"),
        F.last("release_date", ignorenulls=True).over(w_artist_id_by_days_since_rele).alias("last_known_release_date"),
        F.round(F.col("bpm_per_duration_sec") - F.avg("bpm_per_duration_sec").over(w_moods_count), 2).alias("bpm_per_duration_sec_vs_moods_count_avg"),
    )
)

exploded_detail_plays = (
    with_trends.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.bpm",
        "s.days_since_release_date",
        "s.bpm_per_duration_sec",
        "s.title_ref",
        "s.moods_count",
        "s.duration_sec_share_of_moods_count",
        "s.days_since_release_date_share_of_moods_count",
        "s.last_known_release_date",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

result = (
    exploded_detail_plays
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "bpm",
        "days_since_release_date",
        "bpm_per_duration_sec",
        "title_ref",
        "moods_count",
        "duration_sec_share_of_moods_count",
        "days_since_release_date_share_of_moods_count",
        "last_known_release_date",
        "play_played_at",
        F.concat(F.col("title_ref"), F.lit("#"), F.col("play_id").cast("string")).alias("title_ref_ref"),
    )
    .orderBy(F.col("title_ref_ref").asc())
)
