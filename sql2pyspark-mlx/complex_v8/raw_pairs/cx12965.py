from pyspark.sql import functions as F

recent_tickets = (
    df_tickets
    .filter(F.col("priority").isin("P3", "P4"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "tags",
        F.date_trunc("hour", F.col("closed_at")).alias("closed_at_hour"),
    )
)

with_details = (
    recent_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.tags",
        "s.closed_at_hour",
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

scored = (
    with_details
    .select(
        "id",
        "agent_id",
        "closed_at",
        "tags",
        "agent_team",
        "agent_tier",
        "agent_hired_on",
        (F.floor(F.col("requester_id") / 5) * 5).alias("requester_id_bucket"),
        F.coalesce(F.col("requester_id"), F.lit(0)).alias("requester_id_filled"),
    )
)

result = (
    scored
    .groupBy("agent_team", "agent_id")
    .pivot("agent_tier", ["T1", "T2"])
    .agg(F.max("requester_id_filled"))
    .orderBy("agent_team", "agent_id")
)
