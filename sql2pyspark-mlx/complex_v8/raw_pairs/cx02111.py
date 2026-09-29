from pyspark.sql import functions as F

scoped_tickets = (
    df_tickets
    .filter(F.hour(F.col("opened_at")).between(8, 10))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "tags",
        F.when(F.col("priority").isin("P2", "P1"), "group_a").when(F.col("priority") == "P3", "group_b").otherwise("other").alias("priority_group"),
        F.greatest(F.col("csat"), F.col("requester_id")).alias("greatest_csat_requester_id"),
        F.date_trunc("quarter", F.col("closed_at")).alias("closed_at_quarter"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.max("event_at").alias("last_event_at"),
        F.min("event_at").alias("first_event_at"),
    )
)

profiled = (
    scoped_tickets.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        "t.last_event_at",
        "t.first_event_at",
    )
)

without_matches = (
    profiled.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.year(F.col("su.hired_on")) == 2025) & (F.col("su.team").isin("Onboarding", "VIP")), "left_semi")
)

result = (
    without_matches
    .select(
        "id",
        "event_count",
        "greatest_csat_requester_id",
        "requester_id",
    )
    .unpivot(
        ids=["id"],
        values=["event_count", "greatest_csat_requester_id", "requester_id"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
