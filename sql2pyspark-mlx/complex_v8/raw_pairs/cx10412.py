from pyspark.sql import functions as F

scoped = (
    df_tickets
    .filter((F.col("agent_id").isNotNull()) & (F.col("csat") < 5))
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "tags",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
    )
)

joined = (
    scoped.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.tags",
        "s.days_since_closed_at",
        F.col("su.tier").alias("agent_tier"),
    )
)

with_details = (
    joined.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.days_since_closed_at",
        "s.agent_tier",
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
    )
)

result = (
    with_details
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "agent_tier",
        "agent_hired_on",
        "agent_name",
    )
    .orderBy("agent_hired_on")
)
