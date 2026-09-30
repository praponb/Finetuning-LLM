from pyspark.sql import functions as F

prepared_tickets = df_tickets

flattened = (
    prepared_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        F.posexplode_outer("tags").alias("tag_pos", "tag"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count("*").alias("ticket_count"),
        F.count(F.when(F.col("channel").isNotNull(), 1)).alias("ticket_channel_count"),
        F.min("closed_at").alias("first_ticket_closed_at"),
    )
)

profiled = (
    flattened.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        F.coalesce(F.col("t.ticket_channel_count"), F.lit(0)).alias("ticket_channel_count"),
        "t.first_ticket_closed_at",
    )
)

result = (
    profiled
    .filter(~F.col("priority").isin("P3", "P1"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "ticket_count",
        "ticket_channel_count",
        "first_ticket_closed_at",
    )
    .orderBy(F.col("ticket_channel_count").asc())
)
