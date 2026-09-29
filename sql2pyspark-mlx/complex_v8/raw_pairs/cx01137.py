from pyspark.sql import functions as F

cleaned = (
    df_support_agents
    .filter((F.col("hired_on") < F.lit("2025-02-26").cast("date")) & (F.col("team").isin("Onboarding", "VIP")))
    .select(
        "id",
        "team",
        "tier",
        "hired_on",
        F.trunc(F.col("hired_on"), "MM").alias("hired_on_month_start"),
        F.lower(F.regexp_replace(F.trim(F.col("agent_name")), " ", "_")).alias("agent_name_slug"),
    )
)

line_level_tickets = (
    cleaned.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.id")) & (F.col("t.closed_at") < F.lit("2025-02-15 09:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.team",
        "s.tier",
        "s.hired_on",
        "s.hired_on_month_start",
        F.col("t.id").alias("ticket_id"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.priority").alias("ticket_priority"),
    )
)

screened = (
    line_level_tickets
    .crossJoin(line_level_tickets.agg(F.min("ticket_requester_id").alias("overall_min_ticket_requester_id")))
    .filter(F.col("ticket_requester_id") > F.col("overall_min_ticket_requester_id"))
    .drop("overall_min_ticket_requester_id")
)

derived = (
    screened
    .filter(F.col("tier") == "T1")
    .select(
        "id",
        "team",
        "tier",
        "hired_on",
        "hired_on_month_start",
        "ticket_id",
        "ticket_requester_id",
        "ticket_tags",
        "ticket_priority",
        F.coalesce(F.col("ticket_requester_id"), F.lit(0)).alias("ticket_requester_id_filled"),
    )
)

result = (
    derived
    .select(
        "id",
        "team",
        "hired_on",
        "hired_on_month_start",
        "ticket_requester_id",
        "ticket_priority",
        "ticket_requester_id_filled",
        F.concat_ws(" / ", F.col("ticket_priority"), F.col("tier")).alias("ticket_priority_tier_label"),
    )
    .orderBy(F.col("id").asc_nulls_last(), F.col("hired_on").asc_nulls_last())
)
