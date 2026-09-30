from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id").orderBy("played_at", "id")
w_track_id_running = Window.partitionBy("track_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_plays
    .filter(~F.col("is_skipped"))
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

with_prev = base.withColumn("prev_played_at", F.lag("played_at").over(w_track_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_track_id_running))

with_parent_listener = (
    sessionized.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .filter(F.col("l.country").isNotNull())
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.prev_played_at",
        "s.starts_session",
        F.col("l.username").alias("listener_username"),
        F.col("l.country").alias("listener_country"),
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

result = (
    with_parent_listener
    .groupingSets(
        [["starts_session", "track_id", "source"], ["starts_session"]],
        "starts_session", "track_id", "source",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.count(F.when(F.col("device").isin("desktop", "speaker", "car"), 1)).alias("n_device_in_desktop_speaker_ca"),
    )
    .orderBy(
        F.col("starts_session").asc_nulls_last(),
        F.col("track_id").asc_nulls_last(),
        F.col("source").asc_nulls_last(),
    )
)
