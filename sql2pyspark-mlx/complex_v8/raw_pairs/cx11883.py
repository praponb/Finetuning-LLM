from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_running = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_events = (
    df_ticket_events
    .filter(F.col("ticket_id") <= 12)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

with_prev = cleaned_events.withColumn("prev_event_at", F.lag("event_at").over(w_ticket_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_ticket_id_running))

sessions = (
    sessionized
    .groupBy("ticket_id", "burst_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

enriched_ticket = (
    sessions.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.ticket_id",
        "s.burst_id",
        "s.session_start",
        "s.session_end",
        "s.n_events",
        "s.duration_min",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.tags").alias("ticket_tags"),
        "su.agent_name",
    )
)

result = (
    enriched_ticket
    .filter(F.array_contains(F.col("ticket_tags"), "bug"))
    .select(
        "ticket_id",
        "burst_id",
        "session_start",
        "session_end",
        "n_events",
        "duration_min",
        "ticket_agent_id",
        "ticket_tags",
        "agent_name",
        F.length(F.trim(F.col("agent_name"))).alias("agent_name_len"),
    )
    .orderBy(
        "duration_min",
        F.col("session_end").asc(),
        "ticket_id",
        "burst_id",
    )
    .limit(20)
)
