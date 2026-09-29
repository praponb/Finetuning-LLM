from pyspark.sql import functions as F

valid = (
    df_tickets
    .filter(F.col("requester_id").between(4, 25))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "tags",
        F.date_trunc("year", F.col("opened_at")).alias("opened_at_year"),
        F.dayofweek(F.col("closed_at")).alias("closed_at_dayofweek"),
        F.least(F.col("requester_id"), F.col("csat")).alias("least_requester_id_csat"),
    )
)

with_parent = (
    valid.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.team").isin("Billing", "Tech")), "inner")
    .select(
        "s.*",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.max("event_at").alias("last_event_at"))
)

with_counts = (
    with_parent.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select("s.*", "t.last_event_at")
)

result = (
    with_counts
    .filter(F.col("priority") == "P2")
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "tags",
        "opened_at_year",
        "least_requester_id_csat",
        "agent_team",
        "agent_hired_on",
        "agent_tier",
        "last_event_at",
    )
    .orderBy(F.col("agent_hired_on").desc_nulls_first())
)
