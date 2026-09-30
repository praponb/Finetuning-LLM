from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_played_at_ties = Window.orderBy(F.col("played_at").desc())
w_device = Window.partitionBy("device")
w_is_skipped = Window.partitionBy("is_skipped")
w_all_by_played_at = Window.orderBy(F.col("played_at").desc(), "id")
w_device_by_ms_played_full = Window.partitionBy("device").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_ms_played = Window.orderBy("ms_played", "id")
w_ms_played_tier_by_ms_played = Window.partitionBy("ms_played_tier").orderBy(F.col("ms_played").desc())
w_device_by_ms_played_running = Window.partitionBy("device").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ms_played_tier_device_by_ms = Window.partitionBy("ms_played_tier", "device").orderBy("ms_played", "id")
w_track_id_by_ms_played = Window.partitionBy("track_id").orderBy("ms_played", "id")

base_plays = (
    df_plays
    .filter(F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.when(F.col("ms_played") >= 301865, "large").when(F.col("ms_played") >= 112786, "medium").otherwise("small").alias("ms_played_tier"),
    )
)

with_trends = (
    base_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "ms_played_tier",
        F.dense_rank().over(w_all_by_played_at_ties).alias("played_at_dense_rank"),
        F.max("ms_played").over(w_device).alias("device_max_ms_played"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_is_skipped), 2).alias("ms_played_vs_is_skipped_avg"),
        F.row_number().over(w_all_by_played_at).alias("seq_no"),
        F.nth_value("played_at", 2).over(w_device_by_ms_played_full).alias("second_played_at"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_device), F.lit(0)), 4).alias("ms_played_share_of_device"),
        F.row_number().over(w_all_by_ms_played).alias("ms_played_seq"),
        F.round(F.cume_dist().over(w_ms_played_tier_by_ms_played), 3).alias("ms_played_cume_dist"),
        F.sum("ms_played").over(w_device_by_ms_played_running).alias("running_ms_played"),
        F.lead("ms_played", 2, 0).over(w_ms_played_tier_device_by_ms).alias("next2_ms_played"),
        F.round(F.avg("ms_played").over(w_device), 2).alias("device_avg_ms_played"),
        F.row_number().over(w_track_id_by_ms_played).alias("rn"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_device), 2).alias("ms_played_vs_device_avg"),
    )
)

result = (
    with_trends
    .filter(F.col("ms_played_vs_is_skipped_avg") > -240000.0)
    .select(
        "id",
        "ms_played",
        "device",
        "is_skipped",
        "ms_played_tier",
        "played_at_dense_rank",
        "seq_no",
        "second_played_at",
        "ms_played_share_of_device",
        "ms_played_seq",
        "running_ms_played",
        "next2_ms_played",
        "device_avg_ms_played",
        "rn",
        "ms_played_vs_device_avg",
    )
    .orderBy(F.col("ms_played").desc())
)
