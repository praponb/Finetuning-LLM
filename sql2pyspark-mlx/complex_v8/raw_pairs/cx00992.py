from pyspark.sql import functions as F

cleaned = (
    df_tickets
    .filter(F.col("csat") >= 2)
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
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
        F.upper(F.trim(F.col("channel"))).alias("channel_clean"),
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
    )
)

enriched_agent = (
    cleaned.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.length(F.trim(F.col("su.agent_name"))) >= 4)
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.csat",
        "s.tags",
        "s.priority_or_unknown",
        "s.channel_clean",
        "s.csat_filled",
        "su.agent_name",
    )
)

result = (
    enriched_agent
    .select("id", "csat", "csat_filled")
    .unpivot(
        ids=["id"],
        values=["csat", "csat_filled"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
