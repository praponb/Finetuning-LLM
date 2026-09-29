from pyspark.sql import functions as F

filtered = (
    df_ticket_events
    .filter((~F.col("event_type").isin("resolved", "escalated")) | (F.hour(F.col("event_at")).between(8, 14)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.year(F.col("event_at")).alias("event_at_year"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
)

detail_rows_events = (
    filtered.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

joined = (
    detail_rows_events.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.id") == F.col("s.event_id"), "left")
    .join(df_tickets.alias("ti"), F.col("ti.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_id",
        "s.event_actor",
        "s.event_event_at",
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.ticket_id").alias("event_ticket_id"),
        F.col("ti.tags").alias("ticket_tags"),
    )
)

combined_details = (
    joined.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.id") == F.col("s.event_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_id",
        "s.event_actor",
        "s.event_event_at",
        "s.event_ticket_id",
        "s.ticket_tags",
        F.col("t.ticket_id").alias("event_ticket_id_2"),
    )
)

screened = (
    combined_details.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.event_ticket_id"), "left_semi")
    .filter(F.col("event_actor") == "agent")
)

result = (
    screened
    .select(
        "id",
        "event_id",
        "event_actor",
        "event_event_at",
        "event_ticket_id",
        "ticket_tags",
        "event_ticket_id_2",
        F.when(F.col("event_actor").isin("agent", "system"), "group_a").when(F.col("event_actor") == "customer", "group_b").otherwise("other").alias("event_actor_group"),
    )
    .orderBy(F.col("event_actor_group").desc_nulls_first())
)
