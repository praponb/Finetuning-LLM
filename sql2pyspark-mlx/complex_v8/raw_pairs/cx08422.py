from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id")
w_all_by_ms_played = Window.orderBy("ms_played", "id")
w_device_by_ms_played = Window.partitionBy("device").orderBy("ms_played", "id")
w_source_by_ms_played_ties = Window.partitionBy("source").orderBy("ms_played")
w_all_by_ms_played_2 = Window.orderBy(F.col("ms_played").desc(), "id")
w_all_by_ms_played_running = Window.orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id_is_skipped = Window.partitionBy("track_id", "is_skipped")
w_all_by_ms_played_prior_1 = Window.orderBy(F.col("ms_played").desc(), "id").rowsBetween(-1, -1)

recent = (
    df_plays
    .filter(F.col("ms_played") < 220498)
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
    )
)

ranked = (
    recent
    .select(
        "*",
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_listener_id), 2).alias("ms_played_vs_listener_id_avg"),
        F.size(F.collect_set("device").over(w_listener_id)).alias("distinct_device_in_listener_id"),
        F.row_number().over(w_all_by_ms_played).alias("ms_played_seq"),
        (F.col("ms_played") - F.lag("ms_played").over(w_device_by_ms_played)).alias("ms_played_change"),
        F.lead("ms_played", 3).over(w_all_by_ms_played).alias("next3_ms_played"),
        F.dense_rank().over(w_source_by_ms_played_ties).alias("ms_played_dense_rank"),
        (F.col("ms_played") - F.lag("ms_played").over(w_all_by_ms_played_2)).alias("ms_played_change_2"),
        F.first("ms_played", ignorenulls=True).over(w_all_by_ms_played_running).alias("first_known_ms_played"),
        F.sum("ms_played").over(w_track_id_is_skipped).alias("track_id_is_skipped_sum_ms_played"),
        F.round(F.avg("ms_played").over(w_all_by_ms_played_prior_1), 2).alias("ms_played_avg_1_prior"),
        F.count(F.lit(1)).over(w_listener_id).alias("rows_in_listener_id"),
        F.min("ms_played").over(w_track_id_is_skipped).alias("track_id_is_skipped_min_ms_played"),
    )
)

derived = (
    ranked
    .select(
        "id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "ms_played_vs_listener_id_avg",
        "ms_played_seq",
        "ms_played_change",
        "next3_ms_played",
        "ms_played_change_2",
        "first_known_ms_played",
        "track_id_is_skipped_sum_ms_played",
        "rows_in_listener_id",
        "track_id_is_skipped_min_ms_played",
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
    )
)

result = (
    derived
    .filter(F.col("is_skipped"))
    .select(
        "id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "ms_played_vs_listener_id_avg",
        "ms_played_seq",
        "ms_played_change",
        "next3_ms_played",
        "first_known_ms_played",
        "track_id_is_skipped_sum_ms_played",
        "rows_in_listener_id",
        "track_id_is_skipped_min_ms_played",
        "device_or_unknown",
    )
    .orderBy(
        F.col("track_id_is_skipped_min_ms_played").desc_nulls_first(),
        "ms_played_seq",
    )
)
