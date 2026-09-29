from pyspark.sql import functions as F

cleaned = (
    df_tickets
    .filter(F.hour(F.col("closed_at")).between(10, 12))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.min("opened_at").alias("first_ticket_opened_at"),
        F.max("requester_id").alias("max_ticket_requester_id"),
        F.count("*").alias("ticket_count"),
    )
)

with_stats = (
    cleaned.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "t.first_ticket_opened_at",
        "t.max_ticket_requester_id",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

result = (
    with_stats
    .groupingSets(
        [["agent_id", "ticket_count", "priority"], ["agent_id"], []],
        "agent_id", "ticket_count", "priority",
    )
    .agg(
        F.grouping("agent_id").alias("is_total_agent_id"),
        F.sum("ticket_count").alias("total_ticket_count"),
    )
    .select(
        "agent_id",
        "ticket_count",
        F.coalesce(F.col("priority"), F.lit("ALL")).alias("priority"),
        "is_total_agent_id",
        "total_ticket_count",
    )
)
