from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source = Window.partitionBy("source").orderBy("played_at", "id")
w_source_running = Window.partitionBy("source").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

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
    )
)

with_prev = base_plays.withColumn("prev_played_at", F.lag("played_at").over(w_source))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_source_running))

exploded_detail = (
    sessionized.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.source").alias("play_source"),
        F.col("p.track_id").alias("play_track_id"),
    )
)

summary = (
    exploded_detail
    .filter(F.col("device") == "speaker")
    .groupBy("source")
    .agg(
        F.max("ms_played").alias("max_ms_played"),
        F.array_sort(F.collect_set("play_source")).alias("play_source_list"),
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
        F.round(F.avg("ms_played"), 2).alias("avg_ms_played"),
        F.sum("ms_played").alias("total_ms_played"),
    )
)

result = (
    summary
    .filter(F.col("max_ms_played").between(24750, 261750))
    .select(
        "source",
        "max_ms_played",
        "play_source_list",
        "distinct_listener_ids",
        "avg_ms_played",
        "total_ms_played",
        F.abs(F.col("max_ms_played") - 123500).alias("max_ms_played_distance"),
        F.concat(F.col("source"), F.lit("#"), F.col("max_ms_played").cast("string")).alias("source_ref"),
    )
    .orderBy("max_ms_played_distance", F.col("source_ref").desc())
)
