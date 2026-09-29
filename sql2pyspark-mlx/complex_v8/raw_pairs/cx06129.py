from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id").orderBy("played_at", "id")
w_listener_id_running = Window.partitionBy("listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = df_plays

with_prev = filtered.withColumn("prev_played_at", F.lag("played_at").over(w_listener_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_listener_id_running))

per_session = (
    sessionized
    .groupBy("listener_id", "session_no")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("ms_played").alias("session_ms_played"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    per_session
    .cube("session_no", "listener_id")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.sum("event_count").alias("total_event_count"),
        F.max("session_end").alias("last_session_end"),
    )
    .orderBy(
        F.col("session_no").asc_nulls_last(),
        F.col("listener_id").asc_nulls_last(),
    )
)
