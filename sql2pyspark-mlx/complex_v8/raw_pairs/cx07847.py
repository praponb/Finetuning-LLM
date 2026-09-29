from pyspark.sql import functions as F

recent_tickets = (
    df_tickets
    .filter(F.col("csat") >= 1)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        F.concat_ws(" / ", F.col("priority"), F.col("channel")).alias("priority_channel_label"),
    )
)

stats = (
    recent_tickets
    .filter(F.col("channel") == "chat")
    .groupBy("csat", "channel", "agent_id")
    .agg(
        F.round(100 * F.count(F.when(F.col("channel") != "chat", 1)) / F.count("*"), 1).alias("pct_channel_chat"),
        F.max("opened_at").alias("last_opened_at"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("requester_id") / F.nullif(F.sum("csat"), F.lit(0)), 3).alias("requester_id_to_csat_ratio"),
        F.min("opened_at").alias("first_opened_at"),
        F.count("*").alias("cnt"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.min("opened_at").alias("first_ticket_opened_at"),
        F.count("*").alias("ticket_count"),
    )
)

profiled = (
    stats.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "t.first_ticket_opened_at",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

result = (
    profiled
    .select(
        "csat",
        "channel",
        "agent_id",
        "pct_channel_chat",
        "last_opened_at",
        "distinct_ids",
        "cnt",
        "first_ticket_opened_at",
        "ticket_count",
    )
    .orderBy(
        "last_opened_at",
        "csat",
        "channel",
        "agent_id",
    )
    .limit(20)
)
