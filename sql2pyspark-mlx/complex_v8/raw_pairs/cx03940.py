from pyspark.sql import functions as F

cleaned = (
    df_ticket_events
    .filter((F.col("event_at") < F.lit("2025-02-12 12:00:00").cast("timestamp")) & (F.col("event_type").isin("escalated", "assigned", "replied")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.upper(F.trim(F.col("event_type"))).alias("event_type_clean"),
        F.concat_ws("-", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
    )
)

combined_details_ticket = (
    cleaned.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.event_type_clean",
        "s.event_type_actor_label",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

labeled = (
    combined_details_ticket
    .select(
        "id",
        "event_at",
        "ticket_closed_at",
        "ticket_opened_at",
        "ticket_channel",
        F.concat(F.col("ticket_channel"), F.lit("#"), F.col("id").cast("string")).alias("ticket_channel_ref"),
        F.to_date(F.col("ticket_closed_at")).alias("ticket_closed_at_date"),
    )
)

result = (
    labeled
    .filter(F.col("ticket_channel_ref").isNotNull())
    .select(
        "id",
        "event_at",
        "ticket_closed_at",
        "ticket_opened_at",
        "ticket_channel",
        "ticket_channel_ref",
        "ticket_closed_at_date",
    )
    .orderBy(F.col("ticket_closed_at").desc_nulls_first())
)
