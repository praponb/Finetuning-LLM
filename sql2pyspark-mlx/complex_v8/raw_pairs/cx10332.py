from pyspark.sql import functions as F

cleaned_tickets = (
    df_tickets
    .filter((F.col("priority") == "P3") | (F.col("requester_id") <= 15))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        F.date_trunc("year", F.col("opened_at")).alias("opened_at_year"),
        F.date_trunc("hour", F.col("opened_at")).alias("opened_at_hour"),
    )
)

without_matches = (
    cleaned_tickets.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left_semi")
)

combined_details_agent = (
    without_matches.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.opened_at_year",
        "s.opened_at_hour",
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

result = (
    combined_details_agent
    .filter(F.col("opened_at_year") >= F.lit("2025-01-09 09:00:00").cast("timestamp"))
    .select(
        "id",
        "priority",
        "channel",
        "opened_at_year",
        "opened_at_hour",
        "agent_name",
        "agent_team",
        "agent_hired_on",
    )
    .orderBy(F.col("opened_at_year").desc())
)
