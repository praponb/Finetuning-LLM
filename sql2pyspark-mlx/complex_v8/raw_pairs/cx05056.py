from pyspark.sql import functions as F

scoped_tickets = (
    df_tickets
    .filter((F.col("csat").between(2, 4)) & (F.month(F.col("closed_at")).isin(1, 2)))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "tags",
        F.date_format(F.col("closed_at"), "yyyy-MM").alias("closed_at_label"),
    )
)

filtered = (
    scoped_tickets.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.to_date(F.col("t.closed_at")) < F.lit("2025-03-14").cast("date")), "left_anti")
    .filter(F.col("closed_at_label").isNotNull())
)

line_level_events = (
    filtered.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.priority",
        "s.closed_at_label",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        "t.event_at",
        F.col("t.actor").alias("event_actor"),
    )
)

result = (
    line_level_events
    .groupingSets(
        [["event_actor", "priority"], ["event_actor"], ["priority"], []],
        "event_actor", "priority",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("priority").alias("distinct_prioritys"),
    )
    .select(
        F.coalesce(F.col("event_actor"), F.lit("ALL")).alias("event_actor"),
        F.coalesce(F.col("priority"), F.lit("ALL")).alias("priority"),
        "cnt",
        "distinct_prioritys",
    )
    .orderBy(
        F.col("event_actor").asc_nulls_last(),
        F.col("priority").asc_nulls_last(),
    )
)
