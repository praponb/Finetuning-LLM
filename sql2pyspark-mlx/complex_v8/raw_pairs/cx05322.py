from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_events = df_ticket_events.filter(F.col("ticket_id") >= 3)

with_prev = base_events.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_actor_running))

sessions = (
    sessionized
    .groupBy("actor", "session_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

with_activity = (
    sessions
    .crossJoin(sessions.agg(F.avg("session_id").alias("overall_avg_session_id")))
    .filter(F.col("session_id") > F.col("overall_avg_session_id"))
    .drop("overall_avg_session_id")
)

result = (
    with_activity
    .groupingSets(
        [["actor", "session_id", "events"], ["actor"], ["session_id"], []],
        "actor", "session_id", "events",
    )
    .agg(
        F.countDistinct("actor").alias("distinct_actors"),
        F.sum(F.when(F.col("actor") != "agent", F.col("session_id")).otherwise(0)).alias("session_id_actor_agent"),
    )
    .orderBy(
        F.col("actor").asc_nulls_last(),
        F.col("session_id").asc_nulls_last(),
        F.col("events").asc_nulls_last(),
    )
)
