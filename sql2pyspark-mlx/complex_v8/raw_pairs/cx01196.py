from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device = Window.partitionBy("device").orderBy("played_at", "id")
w_device_running = Window.partitionBy("device").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_source_device = Window.partitionBy("source", "device")
w_source_device_by_ms_played = Window.partitionBy("source", "device").orderBy(F.col("ms_played").desc(), "id")
w_source_device_by_ms_played_f = Window.partitionBy("source", "device").orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_new_session_flag_ties = Window.orderBy(F.col("new_session_flag").desc())

scoped_plays = (
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
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
    )
)

with_prev = scoped_plays.withColumn("prev_played_at", F.lag("played_at").over(w_device))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_device_running))

ranked = (
    sessionized
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "source_or_unknown",
        "new_session_flag",
        F.min("session_no").over(w_source_device).alias("source_device_min_session_no"),
        F.size(F.collect_set("track_id").over(w_source_device)).alias("distinct_track_id_in_source_device"),
        F.dense_rank().over(w_source_device_by_ms_played).alias("ms_played_dense_rank"),
        F.nth_value("session_no", 2).over(w_source_device_by_ms_played_f).alias("second_session_no"),
        F.dense_rank().over(w_all_by_new_session_flag_ties).alias("new_session_flag_dense_rank"),
    )
)

result = (
    ranked
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source_or_unknown",
        "new_session_flag",
        "source_device_min_session_no",
        "distinct_track_id_in_source_device",
        "ms_played_dense_rank",
        "second_session_no",
        "new_session_flag_dense_rank",
        F.date_format(F.col("played_at"), "MMM yyyy").alias("played_at_label"),
    )
    .orderBy(
        F.col("device").desc_nulls_first(),
        F.col("distinct_track_id_in_source_device").desc(),
    )
)
