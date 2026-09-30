from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source = Window.partitionBy("source").orderBy("played_at", "id")
w_source_running = Window.partitionBy("source").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_end_month_source_by = Window.partitionBy(F.month("session_end"), "source").orderBy("session_start")
w_session_id = Window.partitionBy("session_id")
w_session_end_month_source_by_2 = Window.partitionBy(F.month("session_end"), "source").orderBy("session_start", "source", "session_id")

valid_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
    )
)

with_prev = valid_plays.withColumn("prev_played_at", F.lag("played_at").over(w_source))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_source_running))

session_summary = (
    sessionized
    .groupBy("source", "session_id")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

with_windows = (
    session_summary
    .select(
        "source",
        "session_id",
        "session_end",
        "n_events",
        "duration_min",
        F.round(F.cume_dist().over(w_session_end_month_source_by), 3).alias("session_start_cume_dist"),
        F.round(F.col("session_id") / F.nullif(F.sum("session_id").over(w_session_id), F.lit(0)), 4).alias("session_id_share_of_session_id"),
        F.count(F.lit(1)).over(w_session_id).alias("rows_in_session_id"),
        F.lag("duration_min").over(w_session_end_month_source_by_2).alias("prev_duration_min"),
    )
)

result = (
    with_windows
    .filter(F.col("source").isNotNull())
    .select(
        "source",
        "session_id",
        "session_end",
        "n_events",
        "duration_min",
        "session_start_cume_dist",
        "session_id_share_of_session_id",
        "rows_in_session_id",
        "prev_duration_min",
        F.substring(F.trim(F.col("source")), 1, 2).alias("source_prefix"),
        F.coalesce(F.col("duration_min"), F.lit(0.0)).alias("duration_min_filled"),
    )
    .orderBy("prev_duration_min", "source", "session_id")
    .limit(10)
)
