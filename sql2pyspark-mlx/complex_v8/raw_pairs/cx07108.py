from pyspark.sql import functions as F

base = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
)

derived = (
    base
    .filter(F.col("event_at") >= F.lit("2025-01-25 08:00:00").cast("timestamp"))
    .select(
        "*",
        F.concat_ws(" / ", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label"),
        F.concat_ws("|", F.col("event_type"), F.col("actor")).alias("event_type_actor_label_2"),
    )
)

with_parent_ticket = (
    derived.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .filter(~F.col("t.priority").isin("P1", "P2"))
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_type_actor_label",
        "s.event_at_label",
        "s.event_type_actor_label_2",
        F.col("t.priority").alias("ticket_priority"),
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
    )
)

result = (
    with_parent_ticket
    .filter(~F.col("event_type").isin("resolved", "assigned"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "event_type_actor_label",
        "event_at_label",
        "event_type_actor_label_2",
        "agent_hired_on",
        "agent_name",
        F.lower(F.regexp_replace(F.trim(F.col("event_type_actor_label")), " ", "_")).alias("event_type_actor_label_slug"),
    )
    .orderBy(F.col("event_at_label").desc(), "id")
    .limit(20)
)
