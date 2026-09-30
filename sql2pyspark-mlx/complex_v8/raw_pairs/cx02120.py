from pyspark.sql import functions as F

scoped = (
    df_tickets
    .filter(F.col("opened_at") < F.lit("2025-03-02 08:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
    )
)

enriched = (
    scoped.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.team").isNotNull()), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
    )
)

joined = (
    enriched.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.tier") == "T3"), "inner")
    .select("s.*", F.col("su.team").alias("agent_team_2"))
)

result = (
    joined
    .filter(F.col("agent_team_2") == "VIP")
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "agent_hired_on",
        "agent_team",
        "agent_team_2",
        F.when(F.col("requester_id") >= 20, "large").when(F.col("requester_id") >= 7, "medium").otherwise("small").alias("requester_id_tier"),
    )
    .orderBy(F.col("agent_id").asc(), F.col("id").desc())
    .limit(10)
)
