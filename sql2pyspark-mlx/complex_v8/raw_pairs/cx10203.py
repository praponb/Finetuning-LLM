from pyspark.sql import functions as F

recent_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.concat(F.col("priority"), F.lit("#"), F.col("requester_id").cast("string")).alias("priority_ref"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.min("event_at").alias("first_event_at"))
)

with_stats = (
    recent_tickets.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select("s.*", "t.first_event_at")
)

rollup_base = (
    with_stats
    .groupBy(
        F.date_trunc("month", "opened_at").alias("opened_at_month"),
        "agent_id",
        "csat",
    )
    .agg(
        F.array_sort(F.collect_set("priority")).alias("priority_list"),
        F.array_sort(F.collect_set("channel")).alias("channel_list"),
        F.min("requester_id").alias("min_requester_id"),
        F.count(F.when(~F.col("channel").isin("email", "web"), 1)).alias("n_channel_not_in_email_web"),
        F.countDistinct("agent_id").alias("distinct_agent_ids"),
    )
)

with_details_agent = (
    rollup_base.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.opened_at_month",
        "s.agent_id",
        "s.csat",
        "s.priority_list",
        "s.channel_list",
        "s.min_requester_id",
        "s.distinct_agent_ids",
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.count("*").alias("ticket_count"))
)

with_counts = (
    with_details_agent.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

result = (
    with_counts
    .select(
        "opened_at_month",
        "agent_id",
        "distinct_agent_ids",
        "csat",
    )
    .unpivot(
        ids=["opened_at_month", "agent_id"],
        values=["distinct_agent_ids", "csat"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .orderBy("opened_at_month", "agent_id", "field")
)
