from pyspark.sql import functions as F

scoped_tickets = (
    df_tickets
    .filter(F.col("requester_id") <= 22)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        "csat",
        "tags",
        F.to_date(F.col("closed_at")).alias("closed_at_date"),
    )
)

combined_details = (
    scoped_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.hired_on").between(F.lit("2024-07-09").cast("date"), F.lit("2025-02-27").cast("date")))
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.closed_at_date",
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

enriched_agent = (
    combined_details.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.channel",
        "s.tags",
        "s.agent_hired_on",
        F.col("su.agent_name").alias("agent_agent_name"),
        F.col("su.hired_on").alias("agent_hired_on_2"),
    )
)

result = (
    enriched_agent
    .select(
        "id",
        "agent_id",
        "closed_at",
        "channel",
        "agent_hired_on",
        "agent_agent_name",
        "agent_hired_on_2",
        F.initcap(F.trim(F.col("agent_agent_name"))).alias("agent_agent_name_clean"),
    )
    .orderBy(F.col("id").desc_nulls_first(), F.col("closed_at").desc_nulls_first())
)
