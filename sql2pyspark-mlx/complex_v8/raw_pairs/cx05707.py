from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device = Window.partitionBy("device").orderBy("played_at", "id")
w_device_running = Window.partitionBy("device").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id_device_by_played_at = Window.partitionBy("track_id", "device").orderBy("played_at", "id")
w_track_id_device_by_played_at_2 = Window.partitionBy("track_id", "device").orderBy("played_at")
w_track_id_device_by_played_at_3 = Window.partitionBy("track_id", "device").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id_device_by_played_at_4 = Window.partitionBy("track_id", "device").orderBy("played_at", "id").rowsBetween(-2, -1)

valid_plays = (
    df_plays
    .filter(F.col("played_at") >= F.lit("2025-02-06 09:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.concat(F.col("source"), F.lit("#"), F.col("ms_played").cast("string")).alias("source_ref"),
    )
)

with_prev = valid_plays.withColumn("prev_played_at", F.lag("played_at").over(w_device))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_device_running))

qualified = (
    sessionized.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.played_at") < F.lit("2025-03-08 10:00:00").cast("timestamp")) & (F.col("p.ms_played") <= 282311), "left_anti")
    .filter(F.col("listener_id").isNotNull())
)

ranked = (
    qualified
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        "source_ref",
        "prev_played_at",
        "session_no",
        F.row_number().over(w_track_id_device_by_played_at).alias("rn"),
        F.rank().over(w_track_id_device_by_played_at_2).alias("played_at_rank"),
        F.lag("ms_played").over(w_track_id_device_by_played_at).alias("prev_ms_played"),
        F.rank().over(w_track_id_device_by_played_at).alias("played_at_rank_2"),
        F.nth_value("played_at", 2).over(w_track_id_device_by_played_at_3).alias("second_played_at"),
        F.round(F.avg("is_new_session").over(w_track_id_device_by_played_at_4), 2).alias("is_new_session_avg_2_prior"),
    )
)

result = (
    ranked
    .select("id", "ms_played", "rn")
    .unpivot(
        ids=["id"],
        values=["ms_played", "rn"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
