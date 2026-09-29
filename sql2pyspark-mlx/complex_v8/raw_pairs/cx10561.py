from pyspark.sql import functions as F

prepared_tickets = (
    df_tickets
    .filter((F.col("channel") == "chat") & (F.col("agent_id").isNotNull()))
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "csat",
        "tags",
        F.least(F.col("csat"), F.col("requester_id")).alias("least_csat_requester_id"),
        F.month(F.col("opened_at")).alias("opened_at_month"),
    )
)

with_details = (
    prepared_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.channel",
        "s.csat",
        "s.opened_at_month",
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
    )
)

result = (
    with_details
    .groupingSets(
        [["agent_id", "csat", "agent_tier"], ["agent_id"], ["csat"], []],
        "agent_id", "csat", "agent_tier",
    )
    .agg(F.countDistinct("channel").alias("distinct_channels"))
    .orderBy(
        F.col("agent_id").asc_nulls_last(),
        F.col("csat").asc_nulls_last(),
        F.col("agent_tier").asc_nulls_last(),
    )
)
