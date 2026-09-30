from pyspark.sql import functions as F

filtered = (
    df_tickets
    .filter((F.month(F.col("closed_at")).isin(2, 3)) & (~F.col("channel").isin("chat", "phone")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "tags",
    )
)

combined_details_agent = (
    filtered.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select("s.*", "su.agent_name", F.col("su.team").alias("agent_team"))
)

with_activity = (
    combined_details_agent.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_at") < F.lit("2025-03-08 09:00:00").cast("timestamp")) & (F.col("t.actor").isin("system", "agent")), "left_semi")
)

result = (
    with_activity
    .groupBy("opened_at", "agent_team")
    .pivot("priority", ["P1", "P3"])
    .agg(F.sum("requester_id").alias("total"), F.max("requester_id").alias("max_v"))
    .orderBy("opened_at", "agent_team")
)
