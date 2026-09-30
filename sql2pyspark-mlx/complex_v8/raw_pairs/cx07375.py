from pyspark.sql import functions as F

prepared = (
    df_ticket_events
    .filter((F.col("actor").isin("customer", "system")) & (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.upper(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
)

combined_details_ticket = (
    prepared.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.priority").isNotNull())
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.event_type_clean",
        F.col("t.priority").alias("ticket_priority"),
    )
)

qualified = (
    combined_details_ticket.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_at") >= F.lit("2025-01-25 10:00:00").cast("timestamp")) & (F.col("t.actor").isNotNull()), "left_anti")
    .filter(F.col("event_type_clean").isNotNull())
)

result = (
    qualified
    .filter(~F.col("ticket_priority").isin("P2", "P4"))
    .select(
        "id",
        "event_at",
        "event_type_clean",
        "ticket_priority",
        F.concat_ws(": ", F.col("ticket_priority"), F.col("event_type_clean")).alias("ticket_priority_event_type_clean_label"),
        F.length(F.trim(F.col("ticket_priority"))).alias("ticket_priority_len"),
    )
    .orderBy("event_type_clean", F.col("event_at").asc())
)
