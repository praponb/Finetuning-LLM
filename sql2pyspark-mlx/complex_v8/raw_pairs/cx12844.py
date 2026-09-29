from pyspark.sql import functions as F

base_events = (
    df_ticket_events
    .filter(F.col("event_type").isin("escalated", "created", "resolved"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.to_date(F.col("event_at")).alias("event_at_date"),
    )
)

enriched = base_events.withColumn("event_at_label", F.date_format(F.col("event_at"), "yyyy-MM-dd"))

with_children = (
    enriched.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

with_details_ticket = (
    with_children.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_id",
        "s.event_event_at",
        "s.event_event_type",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.csat").alias("ticket_csat"),
    )
)

result = (
    with_details_ticket
    .select(
        "id",
        "event_id",
        "ticket_requester_id",
        "ticket_csat",
    )
    .unpivot(
        ids=["id", "event_id"],
        values=["ticket_requester_id", "ticket_csat"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "event_id", "metric")
)
