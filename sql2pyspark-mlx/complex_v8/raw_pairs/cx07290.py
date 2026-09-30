from pyspark.sql import functions as F

recent_tickets = (
    df_tickets
    .filter(F.col("requester_id") >= 2)
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
        F.upper(F.trim(F.col("channel"))).alias("channel_clean"),
        F.array_contains(F.col("tags"), "refund").alias("has_refund"),
        F.round(F.col("csat") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("csat_per_requester_id"),
    )
)

filtered = (
    recent_tickets
    .crossJoin(recent_tickets.agg(F.max("csat").alias("overall_max_csat")))
    .filter(F.col("csat") < F.col("overall_max_csat"))
    .drop("overall_max_csat")
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.max("opened_at").alias("last_ticket_opened_at"),
        F.max("closed_at").alias("last_ticket_closed_at"),
    )
)

with_stats = (
    filtered.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select("s.*", "t.last_ticket_opened_at", "t.last_ticket_closed_at")
)

scored = (
    with_stats
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
        "channel_clean",
        "has_refund",
        "last_ticket_opened_at",
        "last_ticket_closed_at",
        F.concat_ws(": ", F.col("priority"), F.col("channel_clean")).alias("priority_channel_clean_label"),
    )
)

result = (
    scored
    .groupBy("channel", "last_ticket_opened_at")
    .pivot("priority", ["P1", "P2", "P4"])
    .agg(F.min("requester_id").alias("min_v"), F.sum("requester_id").alias("sum_v"))
)
