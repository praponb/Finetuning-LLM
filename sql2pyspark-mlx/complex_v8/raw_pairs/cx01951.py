from pyspark.sql import functions as F

filtered_tickets = (
    df_tickets
    .filter((F.array_contains(F.col("tags"), "outage")) & (F.to_date(F.col("opened_at")) < F.lit("2025-02-24").cast("date")))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "tags",
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
        F.to_date(F.col("opened_at")).alias("opened_at_date"),
        (F.col("csat") - F.col("requester_id")).alias("csat_minus_requester_id"),
    )
)

combined_details_agent = (
    filtered_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.opened_at_date",
        "s.csat_minus_requester_id",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

stats = (
    combined_details_agent
    .groupBy(
        F.date_trunc("month", "agent_hired_on").alias("agent_hired_on_month"),
        "agent_team",
    )
    .agg(
        F.min("opened_at_date").alias("first_opened_at_date"),
        F.sum("csat").alias("total_csat"),
        F.array_sort(F.collect_set("agent_team")).alias("agent_team_list"),
        F.sum("csat_minus_requester_id").alias("total_csat_minus_requester_id"),
        F.max("csat").alias("max_csat"),
    )
)

result = (
    stats
    .groupBy("first_opened_at_date")
    .pivot("agent_team", ["Billing", "VIP", "Tech", "Onboarding"])
    .agg(F.count("max_csat"))
    .orderBy("first_opened_at_date")
)
