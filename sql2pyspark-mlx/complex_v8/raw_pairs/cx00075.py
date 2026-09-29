from pyspark.sql import functions as F

matched = (
    df_tickets.alias("a")
    .join(
        df_tickets.alias("b"),
        (F.col("b.agent_id") == F.col("a.agent_id"))
        & (F.col("b.opened_at") < F.col("a.opened_at"))
        & (F.col("b.opened_at") >= F.col("a.opened_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.agent_id", "a.opened_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.csat").alias("prior_csat_12h"),
    )
)

joined_agent = (
    matched.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.tier").isin("T2", "T1")), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.prior_rows_12h",
        F.col("su.team").alias("agent_team"),
    )
)

annotated = (
    joined_agent
    .select(
        "id",
        "agent_team",
        F.concat(F.col("agent_team"), F.lit("#"), F.col("agent_id").cast("string")).alias("agent_team_ref"),
    )
)

qualified = (
    annotated.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.actor") == "system") & (F.col("t.event_at") >= F.lit("2025-01-13 11:00:00").cast("timestamp")), "left_anti")
)

result = (
    qualified
    .filter(F.col("agent_team_ref").isNotNull())
    .select(
        "id",
        "agent_team",
        "agent_team_ref",
        F.substring(F.trim(F.col("agent_team_ref")), 1, 4).alias("agent_team_ref_prefix"),
        F.coalesce(F.col("agent_team"), F.lit("UNKNOWN")).alias("agent_team_or_unknown"),
    )
    .orderBy("agent_team")
)
