from pyspark.sql import functions as F

scoped = (
    df_tickets
    .filter(F.col("requester_id") > 9)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        F.when(F.col("csat") >= 4, "HIGH").when(F.col("csat") >= 3, "MEDIUM").otherwise("LOW").alias("csat_tier"),
    )
)

with_details_agent = (
    scoped.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.*",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
    )
)

stats = (
    with_details_agent
    .filter(F.col("agent_hired_on").isNotNull())
    .groupBy(
        "agent_id",
        "agent_team",
        F.date_trunc("month", "agent_hired_on").alias("agent_hired_on_month"),
    )
    .agg(
        F.sum("requester_id").alias("total_requester_id"),
        F.round(100 * F.count(F.when(F.col("agent_team").isin("Billing", "Tech", "VIP"), 1)) / F.count("*"), 1).alias("pct_agent_team_in_billing_tech_v"),
        F.round(100 * F.count(F.when(F.col("priority").isNotNull(), 1)) / F.count("*"), 1).alias("pct_priority_is_not_null"),
        F.count(F.when(F.col("agent_team") == "Onboarding", 1)).alias("n_agent_team_onboarding"),
        F.max("requester_id").alias("max_requester_id"),
        F.array_sort(F.collect_set("priority")).alias("priority_list"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count(F.when(F.col("channel").isNotNull(), 1)).alias("ticket_channel_count"),
        F.count("*").alias("ticket_count"),
        F.sum("csat").alias("total_ticket_csat"),
    )
)

summarized_rows = (
    stats.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_channel_count"), F.lit(0)).alias("ticket_channel_count"),
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        "t.total_ticket_csat",
    )
)

result = (
    summarized_rows
    .select(
        "agent_id",
        "agent_team",
        "pct_agent_team_in_billing_tech_v",
        "pct_priority_is_not_null",
    )
    .unpivot(
        ids=["agent_id", "agent_team"],
        values=["pct_agent_team_in_billing_tech_v", "pct_priority_is_not_null"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
)
