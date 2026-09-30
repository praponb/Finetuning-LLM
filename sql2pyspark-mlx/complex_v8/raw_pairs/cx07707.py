from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_source = Window.partitionBy("listener_id", "source")
w_listener_id_by_ms_played = Window.partitionBy("listener_id").orderBy(F.col("ms_played").desc(), "id")
w_device_listener_id = Window.partitionBy("device", "listener_id")
w_all_by_played_at = Window.orderBy("played_at", "id")
w_track_id_is_skipped_by_playe = Window.partitionBy("track_id", "is_skipped").orderBy(F.col("played_at").desc_nulls_first(), "id")
w_all_by_played_at_running = Window.orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_played_at_prior_2 = Window.orderBy("played_at", "id").rowsBetween(-2, -1)
w_listener_id_by_ms_played_run = Window.partitionBy("listener_id").orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_skipped_listener_id_by_ms = Window.partitionBy("is_skipped", "listener_id").orderBy(F.col("ms_played").desc(), "id")
w_played_at_to_date_is_skipped = Window.partitionBy(F.to_date("played_at"), "is_skipped").orderBy(F.col("played_at").desc(), "id")
w_track_id_is_skipped_by_playe_2 = Window.partitionBy("track_id", "is_skipped").orderBy(F.col("played_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

ranked = (
    recent_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_listener_id_source), F.lit(0)), 4).alias("ms_played_share_of_listener_id_source"),
        F.lead("played_at").over(w_listener_id_by_ms_played).alias("next_played_at"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_device_listener_id), F.lit(0)), 4).alias("ms_played_share_of_device_listener_id"),
        F.size(F.collect_set("source").over(w_device_listener_id)).alias("distinct_source_in_device_listener_id"),
        F.lead("ms_played", 3).over(w_all_by_played_at).alias("next3_ms_played"),
        F.ntile(10).over(w_track_id_is_skipped_by_playe).alias("played_at_decile"),
        F.count(F.lit(1)).over(w_all_by_played_at_running).alias("running_count"),
        F.round(F.avg("ms_played").over(w_all_by_played_at_prior_2), 2).alias("ms_played_avg_2_prior"),
        F.sum("ms_played").over(w_listener_id_by_ms_played_run).alias("running_ms_played"),
        F.ntile(4).over(w_is_skipped_listener_id_by_ms).alias("ms_played_quartile"),
        F.dense_rank().over(w_played_at_to_date_is_skipped).alias("played_at_dense_rank"),
        F.min("ms_played").over(w_track_id_is_skipped_by_playe_2).alias("min_ms_played_so_far"),
        F.round(F.percent_rank().over(w_track_id_is_skipped_by_playe), 3).alias("played_at_percent_rank"),
    )
)

result = (
    ranked
    .filter(F.col("running_ms_played") > 1603000)
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        "ms_played_share_of_device_listener_id",
        "distinct_source_in_device_listener_id",
        "next3_ms_played",
        "played_at_decile",
        "ms_played_avg_2_prior",
        "running_ms_played",
        "ms_played_quartile",
        "played_at_dense_rank",
        "min_ms_played_so_far",
        "played_at_percent_rank",
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
    )
    .orderBy(F.col("running_ms_played").asc_nulls_last(), "source")
)
