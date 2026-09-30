from pyspark.sql import functions as F

cleaned = (
    df_tickets
    .filter((F.col("requester_id").between(2, 19)) & (F.col("opened_at") < F.lit("2025-02-18 14:00:00").cast("timestamp")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
        F.to_date(F.col("opened_at")).alias("opened_at_date"),
    )
)

joined = (
    cleaned.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.*",
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
    )
)

derived = (
    joined
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "opened_at_date",
        "agent_team",
        F.to_date(F.col("closed_at")).alias("closed_at_date"),
        (F.col("csat") + F.col("requester_id")).alias("csat_plus_requester_id"),
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
    )
)

with_details_agent = (
    derived.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.hired_on") >= F.lit("2024-07-28").cast("date"))
    .select(
        "s.*",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team_2"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

result = (
    with_details_agent
    .select("id", "csat_filled", "csat_plus_requester_id")
    .unpivot(
        ids=["id"],
        values=["csat_filled", "csat_plus_requester_id"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
