from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device = Window.partitionBy("device")
w_is_skipped_listener_plan_by = Window.partitionBy("is_skipped", "listener_plan").orderBy(F.col("ms_played").desc(), "id")
w_device_listener_id_by_ms_pla = Window.partitionBy("device", "listener_id").orderBy(F.col("ms_played").desc(), "id")
w_is_skipped_track_id_by_playe = Window.partitionBy("is_skipped", "track_id").orderBy(F.col("played_at").desc(), "id")
w_is_skipped_listener_plan_by_2 = Window.partitionBy("is_skipped", "listener_plan").orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_device_listener_id_by_ms_pla_2 = Window.partitionBy("device", "listener_id").orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_device_listener_id_by_ms_pla_3 = Window.partitionBy("device", "listener_id").orderBy(F.col("ms_played").desc(), "id").rowsBetween(-3, -1)
w_is_skipped_listener_plan_by_3 = Window.partitionBy("is_skipped", "listener_plan").orderBy(F.col("ms_played").desc())
w_is_skipped_track_id_by_playe_2 = Window.partitionBy("is_skipped", "track_id").orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_device_listener_id_by_ms_pla_4 = Window.partitionBy("device", "listener_id").orderBy(F.col("ms_played").desc(), "id").rowsBetween(-1, -1)

filtered = (
    df_plays
    .filter((F.col("track_id").isNotNull()) & (F.col("device") == "car"))
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

with_details_listener = (
    filtered.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.signup_date") < F.lit("2024-12-03").cast("date")), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        F.col("l.plan").alias("listener_plan"),
        F.col("l.interests").alias("listener_interests"),
    )
)

with_windows = (
    with_details_listener
    .select(
        "*",
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_device), F.lit(0)), 4).alias("ms_played_share_of_device"),
        F.lag("ms_played").over(w_is_skipped_listener_plan_by).alias("prev_ms_played"),
        F.row_number().over(w_device_listener_id_by_ms_pla).alias("ms_played_seq"),
        (F.col("ms_played") - F.lag("ms_played").over(w_is_skipped_track_id_by_playe)).alias("ms_played_change"),
        F.sum("ms_played").over(w_is_skipped_listener_plan_by_2).alias("running_ms_played"),
        F.min("ms_played").over(w_device_listener_id_by_ms_pla_2).alias("min_ms_played_so_far"),
        F.size(F.collect_set("source").over(w_device)).alias("distinct_source_in_device"),
        F.round(F.avg("ms_played").over(w_device_listener_id_by_ms_pla_3), 2).alias("ms_played_avg_3_prior"),
        F.size(F.collect_set("track_id").over(w_device)).alias("distinct_track_id_in_device"),
        F.size(F.collect_set("listener_plan").over(w_device)).alias("distinct_listener_plan_in_device"),
        F.dense_rank().over(w_is_skipped_listener_plan_by_3).alias("ms_played_dense_rank"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_device), 2).alias("ms_played_vs_device_avg"),
        F.first("played_at", ignorenulls=True).over(w_is_skipped_track_id_by_playe_2).alias("first_known_played_at"),
        F.round(F.avg("ms_played").over(w_device_listener_id_by_ms_pla_4), 2).alias("ms_played_avg_1_prior"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "listener_id",
        "ms_played",
        "device",
        "is_skipped",
        "listener_interests",
        "ms_played_share_of_device",
        "prev_ms_played",
        "ms_played_seq",
        "ms_played_change",
        "min_ms_played_so_far",
        "distinct_source_in_device",
        "ms_played_avg_3_prior",
        "distinct_track_id_in_device",
        "distinct_listener_plan_in_device",
        "ms_played_dense_rank",
        "ms_played_vs_device_avg",
        "first_known_played_at",
    )
)
