from pyspark.sql import functions as F

prepared = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.upper(F.trim(F.col("event_type"))).alias("event_type_clean"),
        F.when(F.col("actor").isin("agent", "customer"), "group_a").when(F.col("actor") == "system", "group_b").otherwise("other").alias("actor_group"),
        F.concat_ws(": ", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
    )
)

with_details = (
    prepared.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.event_type_clean",
        "s.actor_group",
        "s.actor_event_type_label",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
    )
)

with_activity = (
    with_details.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (~F.col("t.actor").isin("system", "customer")) & (F.col("t.event_type").isin("created", "replied")), "left_semi")
)

result = (
    with_activity
    .filter(F.col("ticket_requester_id") <= 21)
    .select(
        "id",
        "event_type",
        "event_type_clean",
        "actor_group",
        "ticket_opened_at",
        "ticket_requester_id",
        "agent_hired_on",
        "agent_name",
        (F.floor(F.col("ticket_requester_id") / 5) * 5).alias("ticket_requester_id_bucket"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("agent_hired_on")).alias("days_since_agent_hired_on"),
    )
    .orderBy(F.col("ticket_opened_at").desc())
)
