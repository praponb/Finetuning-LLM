from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_by_played_at = Window.partitionBy("track_id").orderBy("played_at", "id")
w_is_skipped_track_id_by_ms_pl = Window.partitionBy("is_skipped", "track_id").orderBy(F.col("ms_played_filled").asc_nulls_last())
w_track_id_by_played_at_full = Window.partitionBy("track_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id_by_played_at_runnin = Window.partitionBy("track_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_played_at_moving_2 = Window.orderBy("played_at", "id").rowsBetween(-2, Window.currentRow)
w_device_by_max_ms_played_fill = Window.partitionBy("device").orderBy(F.col("max_ms_played_filled_so_far").desc(), "id")
w_device = Window.partitionBy("device")

cleaned = (
    df_plays
    .filter((F.col("track_id").isNotNull()) & (F.col("device").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
        F.upper(F.trim(F.col("device"))).alias("device_clean"),
    )
)

sequenced = (
    cleaned
    .select(
        "*",
        F.ntile(10).over(w_track_id_by_played_at).alias("played_at_decile"),
        F.lead("ms_played_filled", 2).over(w_track_id_by_played_at).alias("next2_ms_played_filled"),
        F.round(F.cume_dist().over(w_is_skipped_track_id_by_ms_pl), 3).alias("ms_played_filled_cume_dist"),
        F.dense_rank().over(w_track_id_by_played_at).alias("played_at_dense_rank"),
        F.nth_value("played_at", 2).over(w_track_id_by_played_at_full).alias("second_played_at"),
        F.max("ms_played_filled").over(w_track_id_by_played_at_runnin).alias("max_ms_played_filled_so_far"),
        F.round(F.avg("ms_played_filled").over(w_all_by_played_at_moving_2), 2).alias("ms_played_filled_avg_3"),
        F.sum("ms_played_filled").over(w_track_id_by_played_at_runnin).alias("running_ms_played_filled"),
    )
)

tagged = (
    sequenced
    .filter(F.col("listener_id").isNotNull())
    .select(
        "*",
        F.date_format(F.col("second_played_at"), "yyyy-MM").alias("second_played_at_label"),
        F.coalesce(F.col("running_ms_played_filled"), F.lit(0)).alias("running_ms_played_filled_filled"),
    )
)

metrics = (
    tagged
    .select(
        "*",
        F.ntile(10).over(w_device_by_max_ms_played_fill).alias("max_ms_played_filled_so_far_decile"),
        F.round(F.avg("played_at_dense_rank").over(w_device), 2).alias("device_avg_played_at_dense_rank"),
        F.size(F.collect_set("track_id").over(w_device)).alias("distinct_track_id_in_device"),
        F.size(F.collect_set("source").over(w_device)).alias("distinct_source_in_device"),
    )
)

result = (
    metrics
    .select(
        "id",
        "listener_id",
        "device",
        "ms_played_filled",
        "played_at_decile",
        "next2_ms_played_filled",
        "ms_played_filled_cume_dist",
        "played_at_dense_rank",
        "max_ms_played_filled_so_far",
        "running_ms_played_filled",
        "second_played_at_label",
        "max_ms_played_filled_so_far_decile",
        "device_avg_played_at_dense_rank",
        "distinct_track_id_in_device",
        "distinct_source_in_device",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("played_at")).alias("days_since_played_at"),
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
    )
)
