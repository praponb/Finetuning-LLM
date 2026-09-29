from pyspark.sql import functions as F

cleaned_tickets = (
    df_tickets
    .filter((F.col("requester_id") > 11) & (F.col("agent_id").isNotNull()))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
        "tags",
    )
)

derived = (
    cleaned_tickets
    .select(
        "*",
        F.hour(F.col("closed_at")).alias("closed_at_hour"),
        F.month(F.col("closed_at")).alias("closed_at_month"),
    )
)

with_activity = (
    derived.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_at") >= F.lit("2025-02-06 10:00:00").cast("timestamp")), "left_semi")
    .filter(F.array_contains(F.col("tags"), "login"))
)

detail_rows = (
    with_activity.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.priority",
        "s.closed_at_hour",
        "s.closed_at_month",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        F.col("t.actor").alias("event_actor"),
        "t.event_type",
    )
)

result = (
    detail_rows
    .groupingSets(
        [["event_type", "closed_at_month", "closed_at_hour"], ["event_type"], []],
        "event_type", "closed_at_month", "closed_at_hour",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.max("closed_at_hour").alias("max_closed_at_hour"),
    )
    .select(
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        "closed_at_month",
        "closed_at_hour",
        "grouping_level",
        "max_closed_at_hour",
    )
    .orderBy(
        F.col("event_type").asc_nulls_last(),
        F.col("closed_at_month").asc_nulls_last(),
        F.col("closed_at_hour").asc_nulls_last(),
    )
)
