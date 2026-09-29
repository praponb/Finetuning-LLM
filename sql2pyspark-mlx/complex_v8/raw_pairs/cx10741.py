from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_or_unknown = Window.partitionBy("device_or_unknown").orderBy("played_at", "id")
w_device_or_unknown_running = Window.partitionBy("device_or_unknown").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
    )
)

with_prev = base_plays.withColumn("prev_played_at", F.lag("played_at").over(w_device_or_unknown))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_device_or_unknown_running))

sessions = (
    sessionized
    .groupBy("device_or_unknown", "session_no")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    sessions
    .select(
        "device_or_unknown",
        "session_start",
        "session_end",
        "events",
        "duration_min",
        F.coalesce(F.col("duration_min"), F.lit(0.0)).alias("duration_min_filled"),
    )
    .orderBy(F.col("duration_min_filled").desc_nulls_first(), "device_or_unknown")
)
