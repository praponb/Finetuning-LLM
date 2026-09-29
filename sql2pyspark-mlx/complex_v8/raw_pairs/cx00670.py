from pyspark.sql import functions as F

prepared = (
    df_tickets
    .filter(F.col("agent_id") >= 5)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "tags",
        F.when(F.col("requester_id") >= 23, "HIGH").when(F.col("requester_id") >= 7, "MEDIUM").otherwise("LOW").alias("requester_id_tier"),
        F.hour(F.col("closed_at")).alias("closed_at_hour"),
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 3600, 1).alias("hours_opened_at_to_closed_at"),
    )
)

with_activity = (
    prepared.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left_semi")
    .filter(F.col("hours_opened_at_to_closed_at") >= -1200.0)
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.max("closed_at").alias("last_ticket_closed_at"),
        F.count(F.when(~F.col("priority").isin("P1", "P2"), 1)).alias("ticket_priority_count"),
        F.count(F.when(F.col("channel") == "phone", 1)).alias("ticket_channel_count"),
    )
)

profiled = (
    with_activity.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "t.last_ticket_closed_at",
        F.coalesce(F.col("t.ticket_priority_count"), F.lit(0)).alias("ticket_priority_count"),
        F.coalesce(F.col("t.ticket_channel_count"), F.lit(0)).alias("ticket_channel_count"),
    )
)

agent_id_stats = (
    profiled
    .groupBy("agent_id")
    .agg(F.avg("ticket_priority_count").alias("avg_ticket_priority_count"))
)

vs_group = (
    profiled.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.ticket_priority_count") > F.col("g.avg_ticket_priority_count"))
    .select(
        "s.*",
        "g.avg_ticket_priority_count",
        F.round(F.col("s.ticket_priority_count") - F.col("g.avg_ticket_priority_count"), 2).alias("ticket_priority_count_minus_avg"),
    )
)

with_parent_agent = (
    vs_group.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select("s.*", "su.agent_name")
)

with_details = (
    with_parent_agent.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select("s.*", F.col("su.team").alias("agent_team"))
)

grouped = (
    with_details
    .groupBy(
        F.to_date("opened_at").alias("opened_at_day"),
        "closed_at_hour",
        "agent_id",
    )
    .agg(
        F.sum("requester_id").alias("total_requester_id"),
        F.round(F.sum("ticket_priority_count_minus_avg") / F.nullif(F.sum("requester_id"), F.lit(0)), 3).alias("ticket_priority_count_minus_avg_to_requester_id_"),
        F.array_sort(F.collect_set("agent_name")).alias("agent_name_list"),
        F.round(F.avg("ticket_priority_count_minus_avg"), 2).alias("avg_ticket_priority_count_minus_avg"),
    )
)

result = (
    grouped
    .select(
        "opened_at_day",
        "closed_at_hour",
        "avg_ticket_priority_count_minus_avg",
        "ticket_priority_count_minus_avg_to_requester_id_",
    )
    .unpivot(
        ids=["opened_at_day", "closed_at_hour"],
        values=["avg_ticket_priority_count_minus_avg", "ticket_priority_count_minus_avg_to_requester_id_"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
