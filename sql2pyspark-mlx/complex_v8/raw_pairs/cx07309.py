from pyspark.sql import functions as F

base_events = (
    df_ticket_events
    .filter((F.month(F.col("event_at")).isin(1, 2)) & (F.col("event_type").isin("reopened", "replied")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

enriched_ticket = (
    base_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        F.col("t.channel").alias("ticket_channel"),
    )
)

tagged = (
    enriched_ticket
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-03-02").cast("date"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "ticket_channel",
        F.to_date(F.col("event_at")).alias("event_at_date"),
    )
)

result = (
    tagged
    .select(
        "id",
        "ticket_id",
        "event_at",
        "ticket_channel",
        "event_at_date",
        F.when(F.col("ticket_channel").isin("chat", "phone"), "group_a").when(F.col("ticket_channel") == "email", "group_b").otherwise("other").alias("ticket_channel_group"),
        F.initcap(F.trim(F.col("ticket_channel"))).alias("ticket_channel_clean"),
    )
    .orderBy(F.col("id").asc_nulls_last())
)
