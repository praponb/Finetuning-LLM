from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id").orderBy("played_at", "id")
w_track_id_running = Window.partitionBy("track_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = df_plays

with_prev = filtered.withColumn("prev_played_at", F.lag("played_at").over(w_track_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_track_id_running))

combined_details_listener = (
    sessionized.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .filter(F.col("l.birth_year") > 1967)
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.source",
        "s.prev_played_at",
        "s.new_session_flag",
        "s.session_no",
        F.col("l.interests").alias("listener_interests"),
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("l.username").alias("listener_username"),
    )
)

result = (
    combined_details_listener
    .filter(F.col("played_at") >= F.lit("2025-01-19 11:00:00").cast("timestamp"))
    .select(
        "id",
        "played_at",
        "device",
        "new_session_flag",
        "listener_interests",
        "listener_birth_year",
        "listener_username",
    )
    .orderBy(F.col("id").asc())
)
