from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id").orderBy("played_at", "id")
w_listener_id_running = Window.partitionBy("listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_plays = (
    df_plays
    .filter(F.col("device").isin("tv", "desktop"))
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

with_prev = scoped_plays.withColumn("prev_played_at", F.lag("played_at").over(w_listener_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_listener_id_running))

result = (
    sessionized
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        "is_new_session",
        "session_id",
        F.when(F.col("source").isin("album", "search"), "group_a").when(F.col("source") == "radio", "group_b").otherwise("other").alias("source_group"),
    )
    .orderBy(F.col("played_at").asc_nulls_last())
)
