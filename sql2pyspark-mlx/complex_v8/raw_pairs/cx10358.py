from pyspark.sql import functions as F

prepared = (
    df_tickets
    .filter((F.col("agent_id") >= 6) & (F.to_date(F.col("closed_at")) < F.lit("2025-02-24").cast("date")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "tags",
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
        F.date_trunc("month", F.col("opened_at")).alias("opened_at_month"),
    )
)

combined_details = (
    prepared.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.tags",
        "s.opened_at_month",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

result = (
    combined_details
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "tags",
        "opened_at_month",
        "agent_hired_on",
        F.add_months(F.col("agent_hired_on"), 12).alias("agent_hired_on_plus_12m"),
        F.abs(F.col("requester_id") - 8).alias("requester_id_distance"),
    )
    .orderBy(F.col("agent_hired_on_plus_12m").desc())
)
