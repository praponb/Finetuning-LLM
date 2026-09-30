from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_ticket_events
    .filter(~F.col("event_type").isin("reopened", "escalated"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = base.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_actor_running))

combined_details_ticket = (
    sessionized.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.csat") <= 4)
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.prev_event_at",
        "s.new_session_flag",
        "s.burst_id",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

combined_details_ticket_2 = (
    combined_details_ticket.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.prev_event_at",
        "s.new_session_flag",
        "s.burst_id",
        "s.ticket_csat",
        "s.ticket_closed_at",
        "s.ticket_channel",
        F.col("t.channel").alias("ticket_channel_2"),
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

result = (
    combined_details_ticket_2
    .filter(F.col("new_session_flag") < 1)
    .select(
        "id",
        "ticket_id",
        "event_type",
        "prev_event_at",
        "new_session_flag",
        "burst_id",
        "ticket_csat",
        "ticket_closed_at",
        "ticket_channel",
        "ticket_channel_2",
        "agent_team",
        "agent_hired_on",
    )
)
