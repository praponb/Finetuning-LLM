from pyspark.sql import functions as F

prepared = (
    df_ticket_events
    .filter((F.col("ticket_id") <= 38) & (F.col("event_type") == "resolved"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        F.lower(F.trim(F.col("actor"))).alias("actor_clean"),
        F.concat_ws("-", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
    )
)

filtered = (
    prepared.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.opened_at") < F.lit("2025-03-02 08:00:00").cast("timestamp")) & (F.col("t.csat") >= 2), "left_semi")
)

scored = (
    filtered
    .filter(F.col("actor").isin("system", "agent"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "actor_slug",
        "actor_clean",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
)

joined_ticket = (
    scored.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.requester_id").between(2, 18)), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.actor_slug",
        "s.event_type_or_unknown",
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        "su.agent_name",
    )
)

result = (
    joined_ticket
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "actor_slug",
        "ticket_tags",
        "ticket_closed_at",
        "agent_name",
    )
    .orderBy(F.col("ticket_closed_at").asc(), F.col("agent_name").desc())
)
