from pyspark.sql import functions as F

scoped = (
    df_ticket_events
    .filter((F.col("ticket_id") >= 37) & (F.col("event_type") == "reopened"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
        F.concat_ws(" / ", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
    )
)

with_details_ticket = (
    scoped.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.actor",
        "s.event_type_actor_label",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

scored = (
    with_details_ticket
    .select(
        "*",
        F.date_format(F.col("agent_hired_on"), "yyyy-MM-dd").alias("agent_hired_on_label"),
        F.when(F.col("actor").isin("agent", "system"), "group_a").when(F.col("actor") == "customer", "group_b").otherwise("other").alias("actor_group"),
        F.date_trunc("quarter", F.col("agent_hired_on")).alias("agent_hired_on_quarter"),
    )
)

result = (
    scored
    .select(
        "id",
        "actor",
        "event_type_actor_label",
        "ticket_agent_id",
        "agent_hired_on",
        "agent_hired_on_label",
        "actor_group",
        "agent_hired_on_quarter",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("agent_hired_on")).alias("days_since_agent_hired_on"),
        F.year(F.col("agent_hired_on")).alias("agent_hired_on_year"),
    )
    .orderBy(F.col("days_since_agent_hired_on").desc())
)
