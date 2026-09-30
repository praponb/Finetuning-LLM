from pyspark.sql import functions as F

prepared_tickets = (
    df_tickets
    .filter(F.col("requester_id") >= 7)
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
        F.array_join(F.array_sort(F.col("tags")), ",").alias("tags_text"),
    )
)

ticket_side = (
    prepared_tickets
    .filter(F.col("closed_at") < F.lit("2025-02-20 15:00:00").cast("timestamp"))
    .select("csat")
)

agent_side = (
    df_support_agents
    .filter(F.col("hired_on") < F.lit("2025-02-26").cast("date"))
    .select("id")
)

result = (
    ticket_side
    .exceptAll(agent_side)
    .orderBy("csat")
)
