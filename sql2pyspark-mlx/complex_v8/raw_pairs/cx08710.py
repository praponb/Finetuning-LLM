from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source = Window.partitionBy("source").orderBy("played_at", "id")
w_source_running = Window.partitionBy("source").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
    )
)

with_prev = valid_plays.withColumn("prev_played_at", F.lag("played_at").over(w_source))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_source_running))

with_activity = (
    sessionized.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.is_skipped")) & (~F.col("p.device").isin("tv", "desktop")), "left_semi")
)

scored = with_activity.withColumn("source_prefix", F.substring(F.trim(F.col("source")), 1, 4))

result = (
    scored
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "starts_session",
        "session_no",
        "source_prefix",
    )
    .orderBy(F.col("starts_session").desc())
)
