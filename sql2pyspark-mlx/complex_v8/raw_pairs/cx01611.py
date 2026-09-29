from pyspark.sql import functions as F

valid = (
    df_tickets
    .filter(F.col("closed_at") >= F.lit("2025-02-02 03:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "channel",
        "csat",
        F.size(F.col("tags")).alias("tags_count"),
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
    )
)

with_parent_agent = (
    valid.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.length(F.trim(F.col("su.agent_name"))) >= 6)
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.csat",
        "s.channel_slug",
        "su.agent_name",
    )
)

labeled = (
    with_parent_agent
    .filter(F.col("opened_at") < F.lit("2025-02-18 13:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "csat",
        "agent_name",
        F.to_date(F.col("opened_at")).alias("opened_at_date"),
        F.date_trunc("week", F.col("opened_at")).alias("opened_at_week"),
    )
)

result = (
    labeled
    .select(
        "id",
        "requester_id",
        "csat",
        "opened_at_date",
        "opened_at_week",
        F.lower(F.trim(F.col("agent_name"))).alias("agent_name_clean"),
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
    )
    .orderBy(F.col("csat_filled").desc(), F.col("csat").desc(), "id")
    .limit(5)
)
