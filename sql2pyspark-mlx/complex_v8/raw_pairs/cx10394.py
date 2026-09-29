from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id").orderBy("played_at", "id")
w_track_id_running = Window.partitionBy("track_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_device_played_at_to_date_by = Window.partitionBy("device", F.to_date("played_at")).orderBy("prev_played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_device_track_id = Window.partitionBy("device", "track_id")

recent = (
    df_plays
    .filter(~F.col("device").isin("tv", "speaker"))
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

with_prev = recent.withColumn("prev_played_at", F.lag("played_at").over(w_track_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_track_id_running))

sequenced = (
    sessionized
    .select(
        "*",
        F.nth_value("prev_played_at", 2).over(w_device_played_at_to_date_by).alias("second_prev_played_at"),
        F.count(F.lit(1)).over(w_device_track_id).alias("rows_in_device_track_id"),
        F.round(F.avg("burst_id").over(w_device_track_id), 2).alias("device_track_id_avg_burst_id"),
    )
)

listener_id_stats = (
    sequenced
    .groupBy("listener_id")
    .agg(F.percentile_approx("is_new_session", 0.5).alias("median_is_new_session"))
)

above_average = (
    sequenced.alias("s")
    .join(listener_id_stats.alias("g"), F.col("g.listener_id") == F.col("s.listener_id"), "inner")
    .filter(F.col("s.is_new_session") >= F.col("g.median_is_new_session"))
    .select(
        "s.*",
        "g.median_is_new_session",
        F.round(F.col("s.is_new_session") - F.col("g.median_is_new_session"), 2).alias("is_new_session_minus_median"),
    )
)

result = (
    above_average
    .groupBy("prev_played_at")
    .pivot("device", ["tv", "desktop", "speaker", "car"])
    .agg(F.max("rows_in_device_track_id"))
    .orderBy("prev_played_at")
)
