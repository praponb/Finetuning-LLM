from pyspark.sql import functions as F

prepared = (
    df_tickets
    .filter((F.col("agent_id") >= 5) & (F.hour(F.col("opened_at")).between(7, 13)))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 3600, 1).alias("hours_opened_at_to_closed_at"),
    )
)

with_parent_agent = (
    prepared.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.*",
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
    )
)

result = (
    with_parent_agent
    .groupingSets(
        [["agent_id", "csat"], ["agent_id"], []],
        "agent_id", "csat",
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.sum(F.when(F.col("agent_tier") == "T2", F.col("requester_id"))).alias("requester_id_agent_tier_t2"),
        F.max("closed_at").alias("last_closed_at"),
    )
    .orderBy(F.col("agent_id").asc_nulls_last(), F.col("csat").asc_nulls_last())
)
