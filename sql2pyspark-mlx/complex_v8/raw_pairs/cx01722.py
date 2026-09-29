from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_played_at = Window.orderBy("played_at", "id")
w_all_by_played_at_running = Window.orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_device_by_played_at = Window.partitionBy("device").orderBy(F.col("played_at").desc(), "id")
w_device_by_played_at_full = Window.partitionBy("device").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_device_by_played_at_full_2 = Window.partitionBy("device").orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_plays = (
    df_plays
    .filter((F.col("listener_id") <= 18) & (F.col("source") == "album"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
    )
)

with_trends = (
    recent_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        F.row_number().over(w_all_by_played_at).alias("seq_no"),
        F.last("played_at", ignorenulls=True).over(w_all_by_played_at_running).alias("last_known_played_at"),
        F.ntile(10).over(w_device_by_played_at).alias("played_at_decile"),
        F.first("played_at", ignorenulls=True).over(w_all_by_played_at_running).alias("first_known_played_at"),
        F.nth_value("played_at", 2).over(w_device_by_played_at_full).alias("second_played_at"),
        F.nth_value("played_at", 2).over(w_device_by_played_at_full_2).alias("second_played_at_2"),
    )
)

result = (
    with_trends
    .groupingSets(
        [["track_id", "is_skipped"], ["track_id"]],
        "track_id", "is_skipped",
    )
    .agg(
        F.grouping("track_id").alias("is_total_track_id"),
        F.sum(F.when(F.col("device").isin("phone", "speaker", "car"), 1).otherwise(0)).alias("n_device_in_phone_speaker_car"),
    )
    .orderBy(
        F.col("track_id").asc_nulls_last(),
        F.col("is_skipped").asc_nulls_last(),
    )
)
