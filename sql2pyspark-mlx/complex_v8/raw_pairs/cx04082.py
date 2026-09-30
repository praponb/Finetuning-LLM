from pyspark.sql import functions as F

cleaned = (
    df_tickets
    .filter(F.col("requester_id") < 18)
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "tags",
    )
)

tagged = (
    cleaned
    .select(
        "*",
        F.to_date(F.col("closed_at")).alias("closed_at_date"),
        F.dayofweek(F.col("opened_at")).alias("opened_at_dayofweek"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
    )
)

combined_details_agent = (
    tagged.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.year(F.col("su.hired_on")) == 2024), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.csat",
        "s.tags",
        "s.closed_at_date",
        "s.opened_at_dayofweek",
        F.col("su.team").alias("agent_team"),
    )
)

result = (
    combined_details_agent
    .groupBy("opened_at", "opened_at_dayofweek")
    .pivot("agent_team", ["Billing", "Onboarding", "Tech"])
    .agg(F.sum("csat"))
)
