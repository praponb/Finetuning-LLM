from pyspark.sql import functions as F

filtered = (
    df_ticket_events
    .filter((F.to_date(F.col("event_at")) < F.lit("2025-03-08").cast("date")) & (F.col("event_type").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "MMM yyyy").alias("event_at_label"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.lower(F.trim(F.col("actor"))).alias("actor_clean"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.min("event_at").alias("first_event_at"),
        F.max("event_at").alias("last_event_at"),
    )
)

with_stats = (
    filtered.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        "t.first_event_at",
        "t.last_event_at",
    )
)

filtered_2 = (
    with_stats.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type").isNotNull()) & (F.col("t.actor").isNotNull()), "left_semi")
)

result = (
    filtered_2
    .groupBy("event_type", "ticket_id")
    .pivot("event_type_or_unknown", ["UNKNOWN", "assigned", "resolved"])
    .agg(F.count("event_count"))
    .orderBy("event_type", "ticket_id")
)
