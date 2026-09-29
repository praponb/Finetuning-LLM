from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id")
w_is_skipped_by_track_duration = Window.partitionBy("is_skipped").orderBy("track_duration_sec", "id")
w_listener_id_is_skipped_by_ms = Window.partitionBy("listener_id", "is_skipped").orderBy(F.col("ms_played").desc(), "id")
w_listener_id_is_skipped_by_ms_2 = Window.partitionBy("listener_id", "is_skipped").orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_is_skipped_by_ms_3 = Window.partitionBy("listener_id", "is_skipped").orderBy(F.col("ms_played").desc())
w_track_id_listener_id_by_trac = Window.partitionBy("track_id", "listener_id").orderBy("track_duration_sec_share_of_listener_id", "id")
w_ms_played_rank_is_skipped_by = Window.partitionBy("ms_played_rank", "is_skipped").orderBy("listener_id_max_track_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_track_id_in_listene = Window.partitionBy("distinct_track_id_in_listener_id").orderBy("ms_played_tercile", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ms_played_tercile = Window.partitionBy("ms_played_tercile")
w_distinct_track_id_in_listene_2 = Window.partitionBy("distinct_track_id_in_listener_id").orderBy("ms_played_tercile", "id")

prepared = (
    df_plays
    .filter((F.col("device").isNotNull()) | (F.col("listener_id") >= 2))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
    )
)

enriched_track = (
    prepared.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select("s.*", F.col("t.duration_sec").alias("track_duration_sec"))
)

ranked = (
    enriched_track
    .select(
        "*",
        F.size(F.collect_set("track_id").over(w_listener_id)).alias("distinct_track_id_in_listener_id"),
        F.lag("track_duration_sec", 3).over(w_is_skipped_by_track_duration).alias("prev3_track_duration_sec"),
        F.rank().over(w_listener_id_is_skipped_by_ms).alias("ms_played_rank"),
        F.nth_value("played_at", 2).over(w_listener_id_is_skipped_by_ms_2).alias("second_played_at"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_listener_id), 2).alias("ms_played_vs_listener_id_avg"),
        F.ntile(3).over(w_listener_id_is_skipped_by_ms).alias("ms_played_tercile"),
        F.max("track_duration_sec").over(w_listener_id).alias("listener_id_max_track_duration_sec"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_listener_id), F.lit(0)), 4).alias("ms_played_share_of_listener_id"),
        F.sum("track_duration_sec").over(w_listener_id).alias("listener_id_sum_track_duration_sec"),
        F.round(F.cume_dist().over(w_listener_id_is_skipped_by_ms_3), 3).alias("ms_played_cume_dist"),
        F.round(F.col("track_duration_sec") / F.nullif(F.sum("track_duration_sec").over(w_listener_id), F.lit(0)), 4).alias("track_duration_sec_share_of_listener_id"),
    )
)

with_windows = (
    ranked
    .select(
        "*",
        F.round((F.unix_timestamp("second_played_at") - F.unix_timestamp(F.lag("second_played_at").over(w_track_id_listener_id_by_trac))) / 60, 1).alias("minutes_since_prev_second_played_at"),
        F.min("ms_played_rank").over(w_ms_played_rank_is_skipped_by).alias("min_ms_played_rank_so_far"),
        F.last("second_played_at", ignorenulls=True).over(w_distinct_track_id_in_listene).alias("last_known_second_played_at"),
        F.round(F.avg("ms_played_tercile").over(w_ms_played_tercile), 2).alias("ms_played_tercile_avg_ms_played_tercile"),
        F.dense_rank().over(w_distinct_track_id_in_listene_2).alias("ms_played_tercile_dense_rank"),
        F.min("ms_played_vs_listener_id_avg").over(w_ms_played_tercile).alias("ms_played_tercile_min_ms_played_vs_listener_id_a"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
        "prev3_track_duration_sec",
        "ms_played_rank",
        "second_played_at",
        "ms_played_vs_listener_id_avg",
        "ms_played_tercile",
        "ms_played_share_of_listener_id",
        "listener_id_sum_track_duration_sec",
        "ms_played_cume_dist",
        "minutes_since_prev_second_played_at",
        "min_ms_played_rank_so_far",
        "ms_played_tercile_dense_rank",
        "ms_played_tercile_min_ms_played_vs_listener_id_a",
        F.initcap(F.trim(F.col("source"))).alias("source_clean"),
        F.round((F.unix_timestamp(F.col("second_played_at")) - F.unix_timestamp(F.col("played_at"))) / 3600, 1).alias("hours_played_at_to_second_played_at"),
    )
    .orderBy(F.col("source").desc(), "id")
    .limit(20)
)
