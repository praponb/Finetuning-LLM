from pyspark.sql import functions as F

base = (
    df_ticket_events
    .filter((F.col("event_type") != "created") & (F.col("ticket_id") >= 21))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

enriched_ticket = (
    base.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.tags").alias("ticket_tags"),
    )
)

without_matches = (
    enriched_ticket.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.hour(F.col("t.event_at")).between(10, 12)), "left_anti")
)

result = (
    without_matches
    .groupingSets(
        [["actor", "event_type", "ticket_id"], ["actor"], []],
        "actor", "event_type", "ticket_id",
    )
    .agg(
        F.sum(F.when(F.col("actor").isNotNull(), 1).otherwise(0)).alias("n_actor_is_not_null"),
    )
    .orderBy(
        F.col("actor").asc_nulls_last(),
        F.col("event_type").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
    )
)
