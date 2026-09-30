from pyspark.sql import functions as F

prepared = (
    df_tickets
    .filter(F.col("opened_at") < F.lit("2025-03-02 09:00:00").cast("timestamp"))
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

combined_details = (
    prepared.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

combined_details_2 = (
    combined_details.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.hired_on").isNotNull()), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.agent_tier",
        "s.agent_team",
        "s.agent_hired_on",
        F.col("su.tier").alias("agent_tier_2"),
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on_2"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.max("requester_id").alias("max_ticket_requester_id"),
        F.count("*").alias("ticket_count"),
    )
)

with_stats = (
    combined_details_2.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "t.max_ticket_requester_id",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

derived = (
    with_stats
    .filter(F.year(F.col("agent_hired_on")) == 2024)
    .select(
        "*",
        F.when(F.col("agent_tier_2").isin("T3", "T1"), "group_a").when(F.col("agent_tier_2") == "T2", "group_b").otherwise("other").alias("agent_tier_2_group"),
        F.round(F.col("max_ticket_requester_id") / F.nullif(F.col("csat"), F.lit(0)), 3).alias("max_ticket_requester_id_per_csat"),
        F.substring(F.trim(F.col("agent_tier_2")), 1, 3).alias("agent_tier_2_prefix"),
    )
)

with_parent = (
    derived.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.csat",
        "s.agent_hired_on",
        "s.agent_tier_2",
        "s.agent_name",
        "s.agent_hired_on_2",
        "s.max_ticket_requester_id",
        "s.ticket_count",
        "s.agent_tier_2_prefix",
        F.col("su.tier").alias("agent_tier_3"),
        F.col("su.hired_on").alias("agent_hired_on_3"),
    )
)

result = (
    with_parent
    .select(
        "id",
        "agent_id",
        "csat",
        "agent_hired_on",
        "agent_name",
        "ticket_count",
        "agent_tier_2_prefix",
        "agent_tier_3",
        "agent_hired_on_3",
        F.when(F.col("csat") >= 3, "A").when(F.col("csat") >= 2, "B").otherwise("C").alias("csat_tier"),
        F.date_trunc("year", F.col("agent_hired_on")).alias("agent_hired_on_year"),
    )
)
