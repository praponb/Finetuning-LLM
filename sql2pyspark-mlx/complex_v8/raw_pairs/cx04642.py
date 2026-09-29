from pyspark.sql import functions as F

valid = (
    df_tickets
    .filter((F.col("channel").isin("phone", "web")) & (F.size(F.col("tags")) >= 1))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "channel",
        "csat",
        "tags",
        (F.floor(F.col("requester_id") / 5) * 5).alias("requester_id_bucket"),
    )
)

enriched_agent = (
    valid.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.lower(F.col("su.agent_name")).like("%to%"))
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.channel",
        "s.csat",
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
    )
)

with_parent = (
    enriched_agent.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.col("su.hired_on").alias("agent_hired_on_2"),
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

result = (
    with_parent
    .cube("agent_id", "agent_tier", "agent_team")
    .agg(
        F.sum("csat").alias("total_csat"),
        F.sum(F.when(F.col("agent_team") != "Onboarding", F.col("csat")).otherwise(0)).alias("csat_agent_team_onboarding"),
    )
    .select(
        "agent_id",
        F.coalesce(F.col("agent_tier"), F.lit("ALL")).alias("agent_tier"),
        F.coalesce(F.col("agent_team"), F.lit("ALL")).alias("agent_team"),
        "total_csat",
        "csat_agent_team_onboarding",
    )
    .orderBy(
        F.col("agent_id").asc_nulls_last(),
        F.col("agent_tier").asc_nulls_last(),
        F.col("agent_team").asc_nulls_last(),
    )
)
