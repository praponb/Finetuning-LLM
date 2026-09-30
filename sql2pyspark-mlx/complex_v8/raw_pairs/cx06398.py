from pyspark.sql import functions as F

scoped = (
    df_ticket_events
    .filter((F.to_date(F.col("event_at")) < F.lit("2025-02-06").cast("date")) & (F.col("actor") != "agent"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

joined_ticket = (
    scoped.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.max("event_at").alias("last_event_at"))
)

profiled = (
    joined_ticket.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.last_event_at")
)

result = (
    profiled
    .groupingSets(
        [["ticket_channel", "ticket_id"], ["ticket_channel"], []],
        "ticket_channel", "ticket_id",
    )
    .agg(F.max("last_event_at").alias("last_last_event_at"))
    .select(
        F.coalesce(F.col("ticket_channel"), F.lit("ALL")).alias("ticket_channel"),
        "ticket_id",
        "last_last_event_at",
    )
    .orderBy(
        F.col("ticket_channel").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
    )
)
