from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device = Window.partitionBy("device").orderBy("played_at", "id")
w_device_running = Window.partitionBy("device").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_plays
    .filter(F.to_date(F.col("played_at")) < F.lit("2025-02-06").cast("date"))
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

with_prev = filtered.withColumn("prev_played_at", F.lag("played_at").over(w_device))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_device_running))

listener_id_stats = (
    sessionized
    .groupBy("listener_id")
    .agg(F.avg("burst_id").alias("avg_burst_id"))
)

compared = (
    sessionized.alias("s")
    .join(listener_id_stats.alias("g"), F.col("g.listener_id") == F.col("s.listener_id"), "inner")
    .filter(F.col("s.burst_id") > F.col("g.avg_burst_id"))
    .select(
        "s.*",
        "g.avg_burst_id",
        F.round(F.col("s.burst_id") - F.col("g.avg_burst_id"), 2).alias("burst_id_minus_avg"),
    )
)

result = (
    compared
    .groupBy("new_session_flag", "listener_id")
    .pivot("source", ["playlist", "album", "search", "radio"])
    .agg(F.sum("burst_id_minus_avg"))
)
