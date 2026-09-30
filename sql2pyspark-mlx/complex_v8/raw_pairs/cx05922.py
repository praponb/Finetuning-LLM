from pyspark.sql import functions as F

filtered_tickets = (
    df_tickets
    .filter(F.array_contains(F.col("tags"), "login"))
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "tags",
        F.when(F.col("channel").isin("phone", "chat"), "group_a").when(F.col("channel") == "email", "group_b").otherwise("other").alias("channel_group"),
    )
)

combined_details_agent = (
    filtered_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select("s.*", "su.agent_name")
)

result = (
    combined_details_agent
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "tags",
        "agent_name",
        F.initcap(F.trim(F.col("channel_group"))).alias("channel_group_clean"),
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
    )
    .orderBy(
        F.col("channel").asc_nulls_last(),
        F.col("channel_or_unknown").asc_nulls_last(),
    )
)
