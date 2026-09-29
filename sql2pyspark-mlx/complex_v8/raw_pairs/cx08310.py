from pyspark.sql import functions as F

scoped = (
    df_tickets
    .filter(F.col("agent_id").isNotNull())
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        "csat",
        F.hour(F.col("closed_at")).alias("closed_at_hour"),
    )
)

screened = (
    scoped.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.agent_name").isNotNull()), "left_semi")
)

combined_details_agent = (
    screened.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select("s.*", "su.agent_name", F.col("su.team").alias("agent_team"))
)

tagged = (
    combined_details_agent
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        (F.floor(F.col("closed_at_hour") / 2) * 2).alias("closed_at_hour_bucket"),
        F.coalesce(F.col("agent_team"), F.lit("UNKNOWN")).alias("agent_team_or_unknown"),
    )
)

result = (
    tagged
    .cube("agent_id", "closed_at_hour_bucket", "agent_team_or_unknown")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.max("closed_at").alias("last_closed_at"),
        F.array_sort(F.collect_set("channel")).alias("channel_list"),
    )
)
