from pyspark.sql import functions as F

scoped = df_ticket_events.filter(F.col("ticket_id").isNotNull())

joined = (
    scoped.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

result = (
    joined
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "ticket_channel",
        "ticket_closed_at",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.concat_ws("|", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
    )
    .orderBy(F.col("event_type_or_unknown").asc_nulls_last(), "id")
    .limit(20)
)
