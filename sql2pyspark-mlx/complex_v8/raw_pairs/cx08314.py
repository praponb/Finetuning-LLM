from pyspark.sql import functions as F

filtered = (
    df_tickets
    .filter((F.col("csat") < 4) & (F.col("priority").isin("P1", "P4")))
    .select(
        "id",
        "agent_id",
        "channel",
        "csat",
        "tags",
        F.round(F.col("csat") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("csat_per_requester_id"),
        F.hour(F.col("closed_at")).alias("closed_at_hour"),
    )
)

combined_details_agent = (
    filtered.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.channel",
        "s.tags",
        "s.closed_at_hour",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

per_label = (
    combined_details_agent
    .select(
        "id",
        "agent_id",
        "channel",
        "closed_at_hour",
        "agent_hired_on",
        F.explode_outer("tags").alias("tag"),
    )
)

result = (
    per_label
    .groupBy("tag", "agent_hired_on")
    .pivot("channel", ["web", "email", "phone", "chat"])
    .agg(F.count("closed_at_hour"))
)
