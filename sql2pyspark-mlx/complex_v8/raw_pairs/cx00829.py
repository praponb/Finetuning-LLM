from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("play_listener_id").orderBy(F.col("duration_sec_bucket").desc(), F.col("id").desc(), F.col("play_id").desc())
w_latest = Window.partitionBy("artist_id", "play_is_skipped_label").orderBy(F.col("duration_sec").desc(), F.col("id").desc(), F.col("play_id").desc())
w_all_by_release_date_running = Window.orderBy("release_date", "id", "play_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_play_is_skipped_label_releas = Window.partitionBy("play_is_skipped_label", F.year("release_date"))
w_all_by_duration_sec = Window.orderBy(F.col("duration_sec").asc_nulls_last(), "id", "play_id")

valid = (
    df_tracks
    .filter(F.size(F.col("moods")) >= 1)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label"),
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
        F.month(F.col("release_date")).alias("release_date_month"),
    )
)

with_children = (
    valid.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.listener_id") <= 17), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.release_date_label",
        "s.duration_sec_bucket",
        F.col("p.id").alias("play_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

annotated = (
    with_children
    .select(
        "*",
        F.coalesce(F.col("duration_sec_bucket"), F.lit(0)).alias("duration_sec_bucket_filled"),
        F.when(F.col("play_is_skipped"), "yes").otherwise("no").alias("play_is_skipped_label"),
        F.when(F.col("play_is_skipped"), 1).otherwise(0).alias("play_is_skipped_flag"),
    )
)

deduped = (
    annotated
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

latest_per_key = (
    deduped
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

metrics = (
    latest_per_key
    .select(
        "id",
        "title",
        "duration_sec",
        "release_date",
        "release_date_label",
        "duration_sec_bucket",
        "play_id",
        "play_is_skipped",
        "play_listener_id",
        "play_is_skipped_label",
        "play_is_skipped_flag",
        F.first("play_played_at", ignorenulls=True).over(w_all_by_release_date_running).alias("first_known_play_played_at"),
        F.round(F.avg("duration_sec").over(w_play_is_skipped_label_releas), 2).alias("play_is_skipped_label_release_date_year_avg_dura"),
        F.dense_rank().over(w_all_by_duration_sec).alias("duration_sec_dense_rank"),
    )
)

result = (
    metrics
    .filter(F.col("play_listener_id") <= 11)
    .select(
        "id",
        "title",
        "release_date",
        "release_date_label",
        "duration_sec_bucket",
        "play_id",
        "play_is_skipped",
        "play_is_skipped_flag",
        "first_known_play_played_at",
        "play_is_skipped_label_release_date_year_avg_dura",
        F.abs(F.col("duration_sec_dense_rank") - 5).alias("duration_sec_dense_rank_distance"),
    )
    .orderBy(F.col("duration_sec_bucket").desc_nulls_first())
)
