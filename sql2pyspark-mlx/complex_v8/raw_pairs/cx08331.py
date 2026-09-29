from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_year_by_release = Window.partitionBy(F.year("release_date")).orderBy("release_date_month", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_release_date_month_is_explic = Window.partitionBy(F.date_trunc("month", "release_date"), "is_explicit")
w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy(F.col("release_date_month").desc_nulls_first(), "id", "mood_pos").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_release_date_month_is_explic_2 = Window.partitionBy("release_date_month", "is_explicit").orderBy("duration_sec_bucket_bucket", "id", "mood_pos")
w_mood_is_explicit = Window.partitionBy("mood", "is_explicit")
w_release_date_month_is_explic_3 = Window.partitionBy("release_date_month", "is_explicit").orderBy("duration_sec_bucket_bucket", "id", "mood_pos").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_tracks = (
    df_tracks
    .filter((F.col("artist_id").isNotNull()) | (~F.array_contains(F.col("moods"), "sad")))
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.length(F.trim(F.col("title"))).alias("title_len"),
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
        F.abs(F.col("duration_sec") - 213).alias("duration_sec_distance"),
    )
)

enriched = (
    valid_tracks
    .filter(F.col("bpm") >= 82)
    .select(
        "id",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        "duration_sec_bucket",
        "duration_sec_distance",
        F.month(F.col("release_date")).alias("release_date_month"),
        (F.floor(F.col("duration_sec_bucket") / 50) * 50).alias("duration_sec_bucket_bucket"),
        F.coalesce(F.col("bpm"), F.lit(0)).alias("bpm_filled"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.first("bpm").over(w_release_date_year_by_release).alias("first_bpm"),
        F.last("duration_sec_bucket_bucket").over(w_release_date_year_by_release).alias("last_duration_sec_bucket_bucket"),
        F.min("release_date_month").over(w_release_date_month_is_explic).alias("release_date_month_is_explicit_min_release_date_"),
        F.count(F.lit(1)).over(w_release_date_month_is_explic).alias("rows_in_release_date_month_is_explicit"),
        F.first("duration_sec_distance").over(w_release_date_year_by_release).alias("first_duration_sec_distance"),
    )
)

per_label = (
    metrics
    .select(
        "id",
        "release_date",
        "is_explicit",
        "bpm",
        "duration_sec_bucket",
        "duration_sec_distance",
        "release_date_month",
        "duration_sec_bucket_bucket",
        "bpm_filled",
        "first_bpm",
        "last_duration_sec_bucket_bucket",
        "release_date_month_is_explicit_min_release_date_",
        "rows_in_release_date_month_is_explicit",
        "first_duration_sec_distance",
        F.posexplode("moods").alias("mood_pos", "mood"),
    )
)

sequenced = (
    per_label
    .select(
        "*",
        F.first("first_bpm").over(w_is_explicit_by_release_date).alias("first_first_bpm"),
        F.rank().over(w_release_date_month_is_explic_2).alias("duration_sec_bucket_bucket_rank"),
        F.lag("release_date").over(w_release_date_month_is_explic_2).alias("prev_release_date"),
        F.round(F.col("duration_sec_bucket") / F.nullif(F.sum("duration_sec_bucket").over(w_mood_is_explicit), F.lit(0)), 4).alias("duration_sec_bucket_share_of_mood_is_explicit"),
        F.sum("duration_sec_bucket_bucket").over(w_release_date_month_is_explic_3).alias("running_duration_sec_bucket_bucket"),
        F.first("release_date_month", ignorenulls=True).over(w_release_date_month_is_explic_3).alias("first_known_release_date_month"),
    )
)

with_children_plays = (
    sequenced.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.to_date(F.col("p.played_at")) < F.lit("2025-03-02").cast("date")), "inner")
    .select(
        "s.id",
        "s.release_date",
        "s.duration_sec_bucket",
        "s.duration_sec_distance",
        "s.release_date_month",
        "s.duration_sec_bucket_bucket",
        "s.bpm_filled",
        "s.first_bpm",
        "s.last_duration_sec_bucket_bucket",
        "s.rows_in_release_date_month_is_explicit",
        "s.mood_pos",
        "s.duration_sec_bucket_bucket_rank",
        "s.prev_release_date",
        "s.running_duration_sec_bucket_bucket",
        "s.first_known_release_date_month",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.device").alias("play_device"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

result = (
    with_children_plays
    .select(
        "id",
        "duration_sec_bucket",
        "duration_sec_distance",
        "release_date_month",
        "duration_sec_bucket_bucket",
        "bpm_filled",
        "first_bpm",
        "last_duration_sec_bucket_bucket",
        "rows_in_release_date_month_is_explicit",
        "duration_sec_bucket_bucket_rank",
        "prev_release_date",
        "running_duration_sec_bucket_bucket",
        "first_known_release_date_month",
        "play_played_at",
        "play_device",
        F.coalesce(F.col("first_bpm"), F.lit(0)).alias("first_bpm_filled"),
        F.concat(F.col("play_device"), F.lit("#"), F.col("duration_sec_distance").cast("string")).alias("play_device_ref"),
    )
    .orderBy(F.col("duration_sec_bucket").desc_nulls_first(), "prev_release_date")
)
