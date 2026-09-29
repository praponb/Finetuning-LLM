from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source = Window.partitionBy("source").orderBy("played_at", "id")
w_source_running = Window.partitionBy("source").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.concat(F.col("source"), F.lit("#"), F.col("listener_id").cast("string")).alias("source_ref"),
    )
)

with_prev = cleaned.withColumn("prev_played_at", F.lag("played_at").over(w_source))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_source_running))

per_session = (
    sessionized
    .groupBy("source", "session_no")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    per_session
    .select(
        "source",
        "session_no",
        "session_start",
        "session_end",
        "events",
        "duration_min",
    )
    .orderBy("session_end")
)
