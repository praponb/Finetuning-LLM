from pyspark.sql import functions as F

prepared_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

tagged = prepared_events.withColumn("event_at_week", F.date_trunc("week", F.col("event_at")))

with_parent = (
    tagged.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.event_at_week",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

result = (
    with_parent
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_at_week",
        "ticket_csat",
        F.date_trunc("week", F.col("event_at")).alias("event_at_week_2"),
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
    .orderBy("ticket_id")
)
