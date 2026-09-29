from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel").orderBy("closed_at", "id")
w_channel_running = Window.partitionBy("channel").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
    )
)

with_prev = scoped_tickets.withColumn("prev_closed_at", F.lag("closed_at").over(w_channel))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_closed_at").isNull() | (F.col("closed_at") > F.col("prev_closed_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_channel_running))

line_level = (
    sessionized.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.csat",
        "s.prev_closed_at",
        "s.session_id",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_type",
    )
)

result = (
    line_level
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
        "prev_closed_at",
        "session_id",
        "event_id",
        "event_actor",
        "event_type",
        F.concat_ws(": ", F.col("event_actor"), F.col("priority")).alias("event_actor_priority_label"),
        F.coalesce(F.col("event_actor"), F.lit("UNKNOWN")).alias("event_actor_or_unknown"),
    )
    .orderBy("event_actor_priority_label", "event_id")
)
