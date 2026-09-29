from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_artist_id_by_dur = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("duration_sec_per_bpm").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_by_bpm_minus_dur = Window.partitionBy("is_explicit").orderBy(F.col("bpm_minus_duration_sec").desc(), "id")
w_is_explicit_by_bpm_minus_dur_2 = Window.partitionBy("is_explicit").orderBy(F.col("bpm_minus_duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")

recent = (
    df_tracks
    .filter(F.col("release_date") < F.lit("2025-08-02").cast("date"))
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "bpm",
        "moods",
        (F.col("bpm") - F.col("duration_sec")).alias("bpm_minus_duration_sec"),
        F.round(F.col("duration_sec") / F.nullif(F.col("bpm"), F.lit(0)), 3).alias("duration_sec_per_bpm"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("release_date")).alias("days_since_release_date"),
    )
)

ranked = (
    recent
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "bpm",
        "bpm_minus_duration_sec",
        F.first("bpm_minus_duration_sec", ignorenulls=True).over(w_is_explicit_artist_id_by_dur).alias("first_known_bpm_minus_duration_sec"),
        F.lag("duration_sec_per_bpm", 3, 0).over(w_is_explicit_by_bpm_minus_dur).alias("prev3_duration_sec_per_bpm"),
        F.last("bpm").over(w_is_explicit_by_bpm_minus_dur_2).alias("last_bpm"),
        F.round(F.avg("days_since_release_date").over(w_is_explicit_artist_id), 2).alias("is_explicit_artist_id_avg_days_since_release_dat"),
    )
)

detail_rows = (
    ranked.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.is_explicit",
        "s.bpm",
        "s.is_explicit_artist_id_avg_days_since_release_dat",
        F.col("p.id").alias("play_id"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

result = (
    detail_rows
    .cube("artist_id", "play_id")
    .agg(
        F.grouping_id().alias("gid"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("is_explicit"), 1).otherwise(0)).alias("n_is_explicit_true"),
    )
    .orderBy(F.col("artist_id").asc_nulls_last(), F.col("play_id").asc_nulls_last())
)
