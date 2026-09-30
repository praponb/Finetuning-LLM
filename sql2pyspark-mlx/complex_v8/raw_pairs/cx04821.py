from pyspark.sql import functions as F

prepared = (
    df_ticket_events
    .filter(F.col("actor").isin("customer", "system"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws(": ", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
        F.date_format(F.col("event_at"), "yyyy-MM-dd").alias("event_at_label"),
    )
)

filtered = (
    prepared.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor") == "system") & (F.col("t.event_at") >= F.lit("2025-01-25 09:00:00").cast("timestamp")), "left_anti")
    .filter(F.col("event_at_label").isNotNull())
)

with_children_events = (
    filtered.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.to_date(F.col("t.event_at")) < F.lit("2025-03-14").cast("date")), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.actor_event_type_label",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

result = (
    with_children_events
    .filter(F.col("actor_event_type_label").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "actor_event_type_label",
        "event_id",
        "event_event_at",
        "event_actor",
        "event_event_type",
        F.to_date(F.col("event_event_at")).alias("event_event_at_date"),
    )
)
