from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source = Window.partitionBy("source").orderBy("played_at", "id")
w_source_running = Window.partitionBy("source").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
    )
)

with_prev = scoped_plays.withColumn("prev_played_at", F.lag("played_at").over(w_source))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_source_running))

filtered = (
    sessionized.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left_semi")
    .filter(F.month(F.col("played_at")).isin(2, 3))
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.max("ms_played").alias("max_play_ms_played"),
        F.min("played_at").alias("first_played_at"),
    )
)

profiled = (
    filtered.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select("s.*", "p.max_play_ms_played", "p.first_played_at")
)

result = (
    profiled
    .groupBy("track_id")
    .pivot("source", ["search", "radio", "album", "playlist"])
    .agg(F.max("ms_played"))
)
