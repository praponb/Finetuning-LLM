from pyspark.sql import functions as F

prepared_tickets = (
    df_tickets
    .filter(F.col("priority").isin("P3", "P1", "P4"))
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "tags",
        F.to_date(F.col("opened_at")).alias("opened_at_date"),
        F.concat(F.col("priority"), F.lit("#"), F.col("agent_id").cast("string")).alias("priority_ref"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.min("event_at").alias("first_event_at_2"),
        F.min("event_at").alias("first_event_at_3"),
    )
)

with_stats = (
    prepared_tickets.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.first_event_at",
        "t.first_event_at_2",
        "t.first_event_at_3",
    )
)

with_children_events = (
    with_stats.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_type").isin("reopened", "resolved", "escalated")), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.opened_at_date",
        "s.first_event_at",
        "s.first_event_at_3",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        "t.event_type",
    )
)

result = (
    with_children_events
    .groupingSets(
        [["event_type", "agent_id", "event_id"], ["event_type"], ["agent_id"], []],
        "event_type", "agent_id", "event_id",
    )
    .agg(
        F.count(F.when(F.col("event_type") != "reopened", 1)).alias("n_event_type_reopened"),
        F.round(100 * F.count(F.when(F.col("event_type") != "replied", 1)) / F.count("*"), 1).alias("pct_event_type_replied"),
        F.count("*").alias("cnt"),
    )
    .orderBy(
        F.col("event_type").asc_nulls_last(),
        F.col("agent_id").asc_nulls_last(),
        F.col("event_id").asc_nulls_last(),
    )
)
