from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device = Window.partitionBy("device").orderBy("played_at", "id")
w_device_running = Window.partitionBy("device").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_plays = (
    df_plays
    .filter(F.col("device").isin("desktop", "tv", "speaker"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
    )
)

with_prev = scoped_plays.withColumn("prev_played_at", F.lag("played_at").over(w_device))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_device_running))

per_session = (
    sessionized
    .groupBy("device", "session_id")
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
        "device",
        "session_start",
        "session_end",
        "duration_min",
        F.length(F.trim(F.col("device"))).alias("device_len"),
        F.coalesce(F.col("duration_min"), F.lit(0.0)).alias("duration_min_filled"),
    )
    .orderBy(F.col("duration_min_filled").desc(), "device_len")
)
