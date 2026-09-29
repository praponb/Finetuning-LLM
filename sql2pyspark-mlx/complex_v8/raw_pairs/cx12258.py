from pyspark.sql import functions as F

filtered_events = (
    df_ticket_events
    .filter((F.hour(F.col("event_at")).between(7, 12)) & (F.col("ticket_id") >= 36))
)

with_children_events = (
    filtered_events.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

per_group = (
    with_children_events
    .groupBy("ticket_id", "event_type", F.year("event_at").alias("event_at_year"))
    .agg(
        F.sum(F.when(F.col("actor").isin("system", "customer"), 1).otherwise(0)).alias("n_actor_in_system_customer"),
        F.max("event_at").alias("last_event_at"),
        F.count(F.when(F.col("event_event_type") != "created", 1)).alias("n_event_event_type_created"),
        F.min("event_at").alias("first_event_at"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

combined_details_ticket = (
    per_group.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.ticket_id",
        "s.event_type",
        "s.event_at_year",
        "s.n_actor_in_system_customer",
        "s.last_event_at",
        "s.first_event_at",
        "s.distinct_ids",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

result = (
    combined_details_ticket
    .groupBy("distinct_ids")
    .pivot("event_type", ["escalated", "assigned", "created"])
    .agg(F.sum("event_at_year"))
    .orderBy("distinct_ids")
)
