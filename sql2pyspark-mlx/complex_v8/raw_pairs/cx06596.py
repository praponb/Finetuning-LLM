from pyspark.sql import functions as F

prepared = (
    df_ticket_events
    .filter(F.col("event_type").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.year(F.col("event_at")).alias("event_at_year"),
    )
)

with_parent = (
    prepared.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.tags").alias("ticket_tags"),
    )
)

result = (
    with_parent
    .groupBy("event_at", "event_type")
    .pivot("actor", ["customer", "system", "agent"])
    .agg(F.min("event_at_year"))
    .orderBy("event_at", "event_type")
)
