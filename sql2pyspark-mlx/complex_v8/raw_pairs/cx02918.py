from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_by_ms_played = Window.partitionBy("source").orderBy("ms_played", "id")
w_listener_id_source_by_ms_pla = Window.partitionBy("listener_id", "source").orderBy(F.col("ms_played").desc(), "id")
w_is_skipped = Window.partitionBy("is_skipped")
w_source_by_ms_played_full = Window.partitionBy("source").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id_by_ms_played_runnin = Window.partitionBy("track_id").orderBy(F.col("ms_played").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_source_by_ms_played_prior_2 = Window.partitionBy("source").orderBy("ms_played", "id").rowsBetween(-2, -1)
w_track_id_by_ms_played = Window.partitionBy("track_id").orderBy(F.col("ms_played").desc_nulls_first(), "id")

recent_plays = (
    df_plays
    .filter(F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
        F.concat(F.col("device"), F.lit("#"), F.col("listener_id").cast("string")).alias("device_ref"),
    )
)

with_parent = (
    recent_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .filter(F.array_contains(F.col("t.moods"), "sad"))
    .select("s.*", F.col("t.moods").alias("track_moods"))
)

metrics = (
    with_parent
    .select(
        "*",
        F.ntile(3).over(w_source_by_ms_played).alias("ms_played_tercile"),
        F.round(F.percent_rank().over(w_listener_id_source_by_ms_pla), 3).alias("ms_played_percent_rank"),
        F.size(F.collect_set("listener_id").over(w_is_skipped)).alias("distinct_listener_id_in_is_skipped"),
        F.round(F.percent_rank().over(w_source_by_ms_played), 3).alias("ms_played_percent_rank_2"),
        F.last("ms_played").over(w_source_by_ms_played_full).alias("last_ms_played"),
        F.first("ms_played", ignorenulls=True).over(w_track_id_by_ms_played_runnin).alias("first_known_ms_played"),
        F.round(F.avg("ms_played").over(w_source_by_ms_played_prior_2), 2).alias("ms_played_avg_2_prior"),
        F.size(F.collect_set("track_id").over(w_is_skipped)).alias("distinct_track_id_in_is_skipped"),
        F.row_number().over(w_source_by_ms_played).alias("rn"),
        F.round(F.cume_dist().over(w_track_id_by_ms_played), 3).alias("ms_played_cume_dist"),
        (F.col("ms_played") - F.lag("ms_played").over(w_source_by_ms_played)).alias("ms_played_change"),
        F.round(F.cume_dist().over(w_source_by_ms_played), 3).alias("ms_played_cume_dist_2"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_is_skipped), 2).alias("ms_played_vs_is_skipped_avg"),
        F.lag("ms_played", 1, 0).over(w_source_by_ms_played).alias("prev_ms_played"),
    )
)

result = (
    metrics
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
        "device_ref",
        "ms_played_tercile",
        "ms_played_percent_rank",
        "distinct_listener_id_in_is_skipped",
        "ms_played_percent_rank_2",
        "last_ms_played",
        "first_known_ms_played",
        "ms_played_avg_2_prior",
        "rn",
        "ms_played_cume_dist",
        "ms_played_change",
        "ms_played_cume_dist_2",
        "ms_played_vs_is_skipped_avg",
        "prev_ms_played",
    )
)
