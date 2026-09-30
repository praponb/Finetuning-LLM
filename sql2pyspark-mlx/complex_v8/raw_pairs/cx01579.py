from pyspark.sql import functions as F

base_events = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_children_events = (
    base_events.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor") == "customer"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.actor",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

result = (
    with_children_events
    .cube("event_event_type", "ticket_id")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.count("*").alias("row_count"),
    )
    .orderBy(
        F.col("event_event_type").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
    )
)
