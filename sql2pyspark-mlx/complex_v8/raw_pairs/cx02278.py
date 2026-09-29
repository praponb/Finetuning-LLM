from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("source").orderBy(F.col("ms_played").desc(), F.col("id").desc())
w_played_at_quarter_month_trac = Window.partitionBy(F.month("played_at_quarter"), "track_id")
w_is_skipped_device_by_played = Window.partitionBy("is_skipped", "device").orderBy(F.col("played_at_quarter").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_skipped_device_by_played_2 = Window.partitionBy("is_skipped", "device").orderBy(F.col("played_at_quarter").desc())
w_track_id = Window.partitionBy("track_id")
w_played_at_quarter_dense_rank = Window.partitionBy("played_at_quarter_dense_rank", "play_id").orderBy(F.col("ms_played").desc(), "id", "play_id")

recent = (
    df_plays
    .filter((F.col("source").isNotNull()) | (F.col("ms_played") > 160264))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
        F.date_trunc("quarter", F.col("played_at")).alias("played_at_quarter"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

with_activity = (
    recent.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.hour(F.col("p.played_at")).between(7, 13)) & (F.col("p.device").isNotNull()), "left_anti")
)

deduped = (
    with_activity
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_windows = (
    deduped
    .select(
        "id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "is_skipped_flag",
        "played_at_quarter",
        "ms_played_bucket",
        F.max("ms_played").over(w_played_at_quarter_month_trac).alias("played_at_quarter_month_track_id_max_ms_played"),
        F.last("played_at_quarter", ignorenulls=True).over(w_is_skipped_device_by_played).alias("last_known_played_at_quarter"),
        F.dense_rank().over(w_is_skipped_device_by_played_2).alias("played_at_quarter_dense_rank"),
    )
)

detail_rows = (
    with_windows.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.played_at") >= F.lit("2025-01-19 13:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.track_id",
        "s.ms_played",
        "s.is_skipped",
        "s.source",
        "s.is_skipped_flag",
        "s.played_at_quarter_dense_rank",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.listener_id").alias("play_listener_id"),
    )
)

with_trends = (
    detail_rows
    .select(
        "id",
        "track_id",
        "ms_played",
        "source",
        "play_id",
        "play_played_at",
        "play_listener_id",
        F.round(F.col("is_skipped_flag") - F.avg("is_skipped_flag").over(w_track_id), 2).alias("is_skipped_flag_vs_track_id_avg"),
        F.size(F.collect_set("play_listener_id").over(w_track_id)).alias("distinct_play_listener_id_in_track_id"),
        F.sum("played_at_quarter_dense_rank").over(w_track_id).alias("track_id_sum_played_at_quarter_dense_rank"),
        F.round(F.avg("played_at_quarter_dense_rank").over(w_track_id), 2).alias("track_id_avg_played_at_quarter_dense_rank"),
        F.rank().over(w_played_at_quarter_dense_rank).alias("ms_played_rank"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "source",
        "play_id",
        "play_played_at",
        "play_listener_id",
        "is_skipped_flag_vs_track_id_avg",
        "track_id_sum_played_at_quarter_dense_rank",
        "track_id_avg_played_at_quarter_dense_rank",
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug_2"),
    )
    .orderBy(
        F.col("track_id_sum_played_at_quarter_dense_rank").asc_nulls_last(),
        F.col("source").desc_nulls_first(),
    )
)
