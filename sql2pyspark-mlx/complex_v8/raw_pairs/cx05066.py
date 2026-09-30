from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id = Window.partitionBy("artist_id")
w_artist_id_by_bpm_filled_full = Window.partitionBy("artist_id").orderBy(F.col("bpm_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_bpm_per_duration_sec = Window.orderBy(F.col("bpm_per_duration_sec").desc(), "id")

filtered_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

scored = (
    filtered_tracks
    .select(
        "*",
        F.round(F.col("bpm") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("bpm_per_duration_sec"),
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
    )
)

scored_2 = (
    scored
    .filter(F.col("title").isNotNull())
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "moods",
        "bpm_per_duration_sec",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.coalesce(F.col("bpm"), F.lit(0)).alias("bpm_filled"),
    )
)

ranked = (
    scored_2
    .select(
        "*",
        F.round(F.col("bpm_per_duration_sec") / F.nullif(F.sum("bpm_per_duration_sec").over(w_artist_id), F.lit(0)), 4).alias("bpm_per_duration_sec_share_of_artist_id"),
        F.first("duration_sec").over(w_artist_id_by_bpm_filled_full).alias("first_duration_sec"),
        F.min("days_since_release_date").over(w_artist_id).alias("artist_id_min_days_since_release_date"),
        F.lag("days_since_release_date", 3).over(w_all_by_bpm_per_duration_sec).alias("prev3_days_since_release_date"),
    )
)

result = (
    ranked
    .select(
        "id",
        "title",
        "duration_sec",
        "moods",
        "bpm_per_duration_sec",
        "prev3_days_since_release_date",
    )
    .orderBy(F.col("duration_sec").desc())
)
