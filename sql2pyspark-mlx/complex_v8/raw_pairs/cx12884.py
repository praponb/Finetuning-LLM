from pyspark.sql import functions as F

valid = (
    df_ticket_events
    .filter((~F.col("actor").isin("system", "agent")) & (~F.col("event_type").isin("reopened", "escalated")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

qualified = (
    valid.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type") != "assigned") & (F.col("t.actor").isin("system", "agent")), "left_semi")
)

filtered = (
    qualified.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.priority").isNotNull()), "left_semi")
)

result = (
    filtered
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_type_ref"),
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
    .orderBy(F.col("event_type_len").asc(), F.col("actor").desc_nulls_first())
)
