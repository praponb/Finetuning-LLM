from pyspark.sql import functions as F

filtered = (
    df_tickets
    .filter((F.array_contains(F.col("tags"), "outage")) & (F.col("opened_at") < F.lit("2025-02-12 10:00:00").cast("timestamp")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        "tags",
        F.concat_ws("-", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
        F.initcap(F.trim(F.col("channel"))).alias("channel_clean"),
        F.size(F.col("tags")).alias("tags_count"),
    )
)

with_parent_agent = (
    filtered.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.tier") == "T1")
    .select(
        "s.*",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
    )
)

result = (
    with_parent_agent
    .select("id", "tags_count", "csat")
    .unpivot(
        ids=["id"],
        values=["tags_count", "csat"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
)
