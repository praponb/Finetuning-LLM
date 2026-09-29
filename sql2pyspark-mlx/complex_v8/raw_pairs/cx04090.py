from pyspark.sql import functions as F

joined_ranges = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 3 DAYS"))),
        "inner",
    )
    .select(
        "e.id",
        "e.ticket_id",
        "e.event_at",
        "e.event_type",
        "t.priority",
        "t.agent_id",
        F.round((F.unix_timestamp("e.event_at") - F.unix_timestamp("t.opened_at")) / 3600, 2).alias("hours_since_open"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count(F.when(F.col("channel") == "web", 1)).alias("ticket_channel_count"),
        F.count("*").alias("ticket_count"),
        F.max("closed_at").alias("last_ticket_closed_at"),
    )
)

summarized_rows = (
    joined_ranges.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_channel_count"), F.lit(0)).alias("ticket_channel_count"),
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        "t.last_ticket_closed_at",
    )
)

tagged = (
    summarized_rows
    .select(
        "*",
        F.abs(F.col("hours_since_open") - 28.8).alias("hours_since_open_distance"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_type_ref"),
    )
)

result = (
    tagged
    .select(
        "id",
        "event_at",
        "event_type",
        "priority",
        "agent_id",
        "ticket_channel_count",
        "last_ticket_closed_at",
        "event_type_ref",
        F.concat(F.col("priority"), F.lit("#"), F.col("ticket_channel_count").cast("string")).alias("priority_ref"),
    )
    .orderBy(F.col("agent_id").desc())
)
