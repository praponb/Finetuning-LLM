from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("artist_id").orderBy("bpm", "id")
w_latest_2 = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc(), F.col("id").desc())
w_all_by_duration_sec_per_bpm = Window.orderBy("duration_sec_per_bpm", "id")
w_artist_id = Window.partitionBy("artist_id")
w_all_by_duration_sec_per_bpm_2 = Window.orderBy("duration_sec_per_bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_duration_sec_per_bpm_3 = Window.orderBy("duration_sec_per_bpm")
w_duration_sec_per_bpm_quartil = Window.partitionBy("duration_sec_per_bpm_quartile").orderBy("duration_sec_per_bpm_quartile", "id", "play_id")
w_duration_sec_per_bpm_quartil_2 = Window.partitionBy("duration_sec_per_bpm_quartile").orderBy("duration_sec_share_of_artist_id", "id", "play_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_play_listener_id = Window.partitionBy("play_listener_id")

prepared = (
    df_tracks
    .filter(F.size(F.col("moods")) >= 1)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        F.round(F.col("duration_sec") / F.nullif(F.col("bpm"), F.lit(0)), 3).alias("duration_sec_per_bpm"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
)

unique_rows = (
    prepared
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

unique_rows_2 = (
    unique_rows
    .withColumn("recency_rank", F.row_number().over(w_latest_2))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

sequenced = (
    unique_rows_2
    .select(
        "*",
        F.ntile(4).over(w_all_by_duration_sec_per_bpm).alias("duration_sec_per_bpm_quartile"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_id), F.lit(0)), 4).alias("duration_sec_share_of_artist_id"),
        F.nth_value("title_len", 2).over(w_all_by_duration_sec_per_bpm_2).alias("second_title_len"),
        F.round(F.percent_rank().over(w_all_by_duration_sec_per_bpm_3), 3).alias("duration_sec_per_bpm_percent_rank"),
    )
)

with_activity = (
    sequenced.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.played_at") < F.lit("2025-02-18 10:00:00").cast("timestamp")) & (F.col("p.listener_id") <= 6), "left_anti")
    .filter(F.col("is_explicit"))
)

detail_rows = (
    with_activity.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.device") != "speaker"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.is_explicit",
        "s.duration_sec_per_bpm",
        "s.duration_sec_per_bpm_quartile",
        "s.duration_sec_share_of_artist_id",
        "s.duration_sec_per_bpm_percent_rank",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.device").alias("play_device"),
        F.col("p.listener_id").alias("play_listener_id"),
    )
)

metrics = (
    detail_rows
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "duration_sec_per_bpm",
        "duration_sec_per_bpm_quartile",
        "play_id",
        "play_source",
        "play_listener_id",
        (F.col("duration_sec_per_bpm") - F.lag("duration_sec_per_bpm").over(w_duration_sec_per_bpm_quartil)).alias("duration_sec_per_bpm_change"),
        F.last("duration_sec_per_bpm", ignorenulls=True).over(w_duration_sec_per_bpm_quartil_2).alias("last_known_duration_sec_per_bpm"),
        F.size(F.collect_set("play_device").over(w_play_listener_id)).alias("distinct_play_device_in_play_listener_id"),
        (F.col("duration_sec") - F.lag("duration_sec").over(w_duration_sec_per_bpm_quartil)).alias("duration_sec_change"),
        F.lead("duration_sec_per_bpm", 3).over(w_duration_sec_per_bpm_quartil).alias("next3_duration_sec_per_bpm"),
    )
)

result = (
    metrics
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "duration_sec_per_bpm",
        "duration_sec_per_bpm_quartile",
        "play_source",
        "play_listener_id",
        "duration_sec_per_bpm_change",
        "last_known_duration_sec_per_bpm",
        "distinct_play_device_in_play_listener_id",
        "duration_sec_change",
        F.substring(F.trim(F.col("play_source")), 1, 3).alias("play_source_prefix"),
    )
)
