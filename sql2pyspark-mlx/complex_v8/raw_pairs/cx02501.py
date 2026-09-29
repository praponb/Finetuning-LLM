from pyspark.sql import functions as F

cleaned_agents = (
    df_support_agents
    .filter((F.col("tier") != "T2") & (F.col("team") != "Onboarding"))
    .select(
        "id",
        "team",
        "tier",
        "hired_on",
        F.coalesce(F.col("tier"), F.lit("UNKNOWN")).alias("tier_or_unknown"),
    )
)

with_activity = (
    cleaned_agents.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.id")) & (F.col("t.priority") == "P2") & (F.col("t.opened_at") < F.lit("2025-02-12 14:00:00").cast("timestamp")), "left_anti")
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count("*").alias("ticket_count"),
        F.min("opened_at").alias("first_ticket_opened_at"),
        F.max("closed_at").alias("last_ticket_closed_at"),
    )
)

profiled = (
    with_activity.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        "t.first_ticket_opened_at",
        "t.last_ticket_closed_at",
    )
)

without_matches = (
    profiled
    .crossJoin(profiled.agg(F.min("ticket_count").alias("overall_min_ticket_count")))
    .filter(F.col("ticket_count") > F.col("overall_min_ticket_count"))
    .drop("overall_min_ticket_count")
)

derived = (
    without_matches
    .select(
        "id",
        "team",
        "tier",
        "hired_on",
        "tier_or_unknown",
        "ticket_count",
        F.substring(F.trim(F.col("tier_or_unknown")), 1, 2).alias("tier_or_unknown_prefix"),
    )
)

result = (
    derived
    .filter(F.col("tier_or_unknown_prefix").isNotNull())
    .select(
        "id",
        "tier_or_unknown",
        "ticket_count",
        "tier_or_unknown_prefix",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("hired_on")).alias("days_since_hired_on"),
        F.coalesce(F.col("ticket_count"), F.lit(0)).alias("ticket_count_filled"),
    )
    .orderBy("days_since_hired_on", "tier_or_unknown_prefix", "id")
    .limit(10)
)
