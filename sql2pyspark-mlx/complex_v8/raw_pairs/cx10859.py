from pyspark.sql import functions as F

valid_events = (
    df_ticket_events
    .filter((F.col("event_at") >= F.lit("2025-01-13 14:00:00").cast("timestamp")) & (F.col("event_type") == "reopened"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        (F.col("event_at") + F.expr("INTERVAL 1 DAYS")).alias("event_at_plus_1d"),
        F.date_trunc("year", F.col("event_at")).alias("event_at_year"),
    )
)

combined_details = (
    valid_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.requester_id") <= 23)
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.event_at_plus_1d",
        "s.event_at_year",
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.priority").alias("ticket_priority"),
    )
)

event_type_stats = (
    combined_details
    .groupBy("event_type")
    .agg(F.percentile_approx("ticket_requester_id", 0.5).alias("median_ticket_requester_id"))
)

above_average = (
    combined_details.alias("s")
    .join(event_type_stats.alias("g"), F.col("g.event_type") == F.col("s.event_type"), "inner")
    .filter(F.col("s.ticket_requester_id") >= F.col("g.median_ticket_requester_id"))
    .select(
        "s.*",
        "g.median_ticket_requester_id",
        F.round(F.col("s.ticket_requester_id") - F.col("g.median_ticket_requester_id"), 2).alias("ticket_requester_id_minus_median"),
    )
)

combined_details_2 = (
    above_average.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.*",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.requester_id").alias("ticket_requester_id_2"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

result = (
    combined_details_2
    .select(
        "id",
        "event_type",
        "actor",
        "event_at_plus_1d",
        "ticket_requester_id",
        "ticket_priority",
        "median_ticket_requester_id",
        "ticket_csat",
        "ticket_requester_id_2",
        "ticket_channel",
    )
    .orderBy(F.col("ticket_priority").desc_nulls_first(), "event_at_plus_1d", "id")
    .limit(20)
)
