from pyspark.sql import functions as F

base_tickets = (
    df_tickets
    .filter((F.col("channel") == "email") & (F.col("csat") > 2))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "tags",
        F.when(F.col("priority").isin("P4", "P1"), "group_a").when(F.col("priority") == "P3", "group_b").otherwise("other").alias("priority_group"),
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 3600, 1).alias("hours_opened_at_to_closed_at"),
    )
)

qualified = (
    base_tickets.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.team") == "Onboarding") & (F.col("su.hired_on").between(F.lit("2024-05-02").cast("date"), F.lit("2025-11-09").cast("date"))), "left_anti")
)

screened = (
    qualified.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_type").isNotNull()) & (F.col("t.actor").isNotNull()), "left_anti")
    .filter(F.size(F.col("tags")) >= 1)
)

per_label = (
    screened
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "priority_group",
        "hours_opened_at_to_closed_at",
        F.explode_outer("tags").alias("tag"),
    )
)

enriched = (
    per_label.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(~F.col("su.team").isin("Billing", "Tech"))
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.priority_group",
        "s.hours_opened_at_to_closed_at",
        "s.tag",
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
    )
)

result = (
    enriched
    .select(
        "id",
        "requester_id",
        "priority_group",
        "hours_opened_at_to_closed_at",
        "tag",
        "agent_name",
        "agent_team",
        F.coalesce(F.col("agent_team"), F.lit("UNKNOWN")).alias("agent_team_or_unknown"),
        F.when(F.col("agent_team").isin("VIP", "Onboarding"), "group_a").when(F.col("agent_team") == "Tech", "group_b").otherwise("other").alias("agent_team_group"),
    )
    .orderBy("agent_name", F.col("tag").asc_nulls_last())
)
