from pyspark.sql import functions as F

scoped_events = (
    df_ticket_events
    .filter(F.col("event_type").isin("escalated", "resolved"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.initcap(F.trim(F.col("actor"))).alias("actor_clean"),
        F.dayofweek(F.col("event_at")).alias("event_at_dayofweek"),
    )
)

labeled = (
    scoped_events
    .select(
        "*",
        F.quarter(F.col("event_at")).alias("event_at_quarter"),
        F.coalesce(F.col("event_at_dayofweek"), F.lit(0)).alias("event_at_dayofweek_filled"),
    )
)

with_parent_ticket = (
    labeled.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        F.col("t.priority").alias("ticket_priority"),
    )
)

detail_rows_events = (
    with_parent_ticket.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.ticket_priority",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        "t.event_at",
    )
)

result = (
    detail_rows_events
    .groupingSets(
        [["event_type", "actor", "ticket_priority"], ["event_type"], ["actor"], []],
        "event_type", "actor", "ticket_priority",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
    )
)
