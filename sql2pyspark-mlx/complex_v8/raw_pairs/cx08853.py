from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source = Window.partitionBy("source").orderBy("played_at", "id")
w_source_running = Window.partitionBy("source").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_plays
    .filter(F.col("played_at") < F.lit("2025-03-02 09:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

with_prev = base.withColumn("prev_played_at", F.lag("played_at").over(w_source))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_source_running))

line_level = (
    sessionized.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.source",
        "s.days_since_played_at",
        "s.prev_played_at",
        "s.new_session_flag",
        "s.session_id",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

result = (
    line_level
    .groupBy("play_id")
    .pivot("device", ["tv", "car", "phone", "speaker"])
    .agg(F.max("days_since_played_at").alias("max_v"), F.count("days_since_played_at").alias("n"))
    .orderBy("play_id")
)
