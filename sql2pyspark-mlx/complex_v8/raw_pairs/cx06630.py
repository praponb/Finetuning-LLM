from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id").orderBy("played_at", "id")
w_listener_id_running = Window.partitionBy("listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_device_by_starts_session_ful = Window.partitionBy("device").orderBy("starts_session", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id_by_days_since_playe = Window.partitionBy("track_id").orderBy(F.col("days_since_played_at").desc())
w_device_by_starts_session_run = Window.partitionBy("device").orderBy("starts_session", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_device_by_starts_session = Window.partitionBy("device").orderBy("starts_session", "id")
w_track_id_starts_session = Window.partitionBy("track_id", "starts_session")
w_all_by_starts_session_ties = Window.orderBy("starts_session")

filtered_plays = (
    df_plays
    .filter(~F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

with_prev = filtered_plays.withColumn("prev_played_at", F.lag("played_at").over(w_listener_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_listener_id_running))

sequenced = (
    sessionized
    .select(
        "*",
        F.last("played_at").over(w_device_by_starts_session_ful).alias("last_played_at"),
        F.dense_rank().over(w_track_id_by_days_since_playe).alias("days_since_played_at_dense_rank"),
        F.min("starts_session").over(w_device_by_starts_session_run).alias("min_starts_session_so_far"),
        F.dense_rank().over(w_device_by_starts_session).alias("starts_session_dense_rank"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_track_id_starts_session), F.lit(0)), 4).alias("ms_played_share_of_track_id_starts_session"),
        F.dense_rank().over(w_all_by_starts_session_ties).alias("starts_session_dense_rank_2"),
    )
)

result = (
    sequenced
    .filter(F.col("days_since_played_at_dense_rank") <= 9)
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        "prev_played_at",
        "starts_session",
        "session_no",
        "last_played_at",
        "starts_session_dense_rank",
        "ms_played_share_of_track_id_starts_session",
        "starts_session_dense_rank_2",
        F.substring(F.trim(F.col("device")), 1, 2).alias("device_prefix"),
        F.when(F.col("days_since_played_at") >= 595, "A").when(F.col("days_since_played_at") >= 210, "B").otherwise("C").alias("days_since_played_at_tier"),
    )
    .orderBy(F.col("track_id").desc())
)
