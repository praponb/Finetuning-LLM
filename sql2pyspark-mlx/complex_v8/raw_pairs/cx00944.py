from pyspark.sql import functions as F

valid_tickets = (
    df_tickets
    .filter((F.col("channel") != "chat") & (F.col("requester_id") < 17))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

agent_id_stats = (
    valid_tickets
    .groupBy("agent_id")
    .agg(F.avg("csat").alias("avg_csat"))
)

compared = (
    valid_tickets.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.csat") > F.col("g.avg_csat"))
    .select(
        "s.*",
        "g.avg_csat",
        F.round(F.col("s.csat") - F.col("g.avg_csat"), 2).alias("csat_minus_avg"),
    )
)

without_matches = (
    compared
    .crossJoin(compared.agg(F.min("requester_id").alias("overall_min_requester_id")))
    .filter(F.col("requester_id") > F.col("overall_min_requester_id"))
    .drop("overall_min_requester_id")
)

with_children = (
    without_matches.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.tags",
        "s.avg_csat",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_type",
        "t.event_at",
    )
)

result = (
    with_children
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "channel",
        "tags",
        "avg_csat",
        "event_id",
        "event_actor",
        "event_type",
    )
    .orderBy(F.col("requester_id").desc())
)
