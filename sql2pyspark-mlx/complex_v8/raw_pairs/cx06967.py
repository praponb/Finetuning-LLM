from pyspark.sql import functions as F

scoped_events = (
    df_ticket_events
    .filter((F.col("actor").isin("customer", "agent")) | (~F.col("event_type").isin("assigned", "created")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

joined = (
    scoped_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        F.col("t.csat").alias("ticket_csat"),
    )
)

screened = (
    joined.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left_semi")
)

result = (
    screened
    .groupingSets(
        [["ticket_csat", "ticket_id", "event_type"], ["ticket_csat"], []],
        "ticket_csat", "ticket_id", "event_type",
    )
    .agg(
        F.sum("ticket_csat").alias("total_ticket_csat"),
        F.min("ticket_csat").alias("min_ticket_csat"),
        F.countDistinct("event_type").alias("distinct_event_types"),
    )
    .orderBy(
        F.col("ticket_csat").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
        F.col("event_type").asc_nulls_last(),
    )
)
