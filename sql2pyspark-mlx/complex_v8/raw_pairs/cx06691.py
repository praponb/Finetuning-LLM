from pyspark.sql import functions as F

cleaned_events = (
    df_ticket_events
    .filter((F.hour(F.col("event_at")).between(8, 10)) & (F.col("actor").isin("agent", "system")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("event_type").isin("resolved", "assigned"), "group_a").when(F.col("event_type") == "created", "group_b").otherwise("other").alias("event_type_group"),
    )
)

with_parent = (
    cleaned_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

result = (
    with_parent
    .cube("ticket_channel", "ticket_id", "event_type")
    .agg(
        F.grouping("ticket_channel").alias("is_total_ticket_channel"),
        F.count("*").alias("row_count"),
        F.max("ticket_requester_id").alias("max_ticket_requester_id"),
    )
    .select(
        F.coalesce(F.col("ticket_channel"), F.lit("ALL")).alias("ticket_channel"),
        "ticket_id",
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        "is_total_ticket_channel",
        "row_count",
        "max_ticket_requester_id",
    )
    .orderBy(
        F.col("ticket_channel").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
        F.col("event_type").asc_nulls_last(),
    )
)
