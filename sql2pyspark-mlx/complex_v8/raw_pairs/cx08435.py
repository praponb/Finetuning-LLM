from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device = Window.partitionBy("device").orderBy("played_at", "id")
w_device_running = Window.partitionBy("device").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_events_device_by_duration_mi = Window.partitionBy("events", "device").orderBy("duration_min")
w_device_2 = Window.partitionBy("device")

base_plays = (
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
        F.length(F.trim(F.col("source"))).alias("source_len"),
    )
)

with_prev = base_plays.withColumn("prev_played_at", F.lag("played_at").over(w_device))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_device_running))

session_summary = (
    sessionized
    .groupBy("device", "session_id")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("source_len").alias("session_source_len"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

windowed = (
    session_summary
    .select(
        "device",
        "session_id",
        "session_start",
        "session_end",
        "duration_min",
        F.round(F.cume_dist().over(w_events_device_by_duration_mi), 3).alias("duration_min_cume_dist"),
        F.round(F.percent_rank().over(w_events_device_by_duration_mi), 3).alias("duration_min_percent_rank"),
        F.count(F.lit(1)).over(w_device_2).alias("rows_in_device"),
    )
)

result = (
    windowed
    .filter(F.month(F.col("session_start")).isin(1, 2))
    .select(
        "device",
        "session_start",
        "session_end",
        "duration_min",
        "duration_min_percent_rank",
        "rows_in_device",
    )
)
