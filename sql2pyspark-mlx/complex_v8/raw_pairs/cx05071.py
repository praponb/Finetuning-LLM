from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device = Window.partitionBy("device").orderBy("played_at", "id")
w_device_running = Window.partitionBy("device").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_plays = (
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
        F.year(F.col("played_at")).alias("played_at_year"),
    )
)

with_prev = filtered_plays.withColumn("prev_played_at", F.lag("played_at").over(w_device))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_device_running))

per_session = (
    sessionized
    .groupBy("device", "burst_id")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("ms_played").alias("session_ms_played"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    per_session
    .select(
        "device",
        "burst_id",
        "event_count",
        "session_ms_played",
    )
    .unpivot(
        ids=["device", "burst_id"],
        values=["event_count", "session_ms_played"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .orderBy("device", "burst_id", "measure")
)
