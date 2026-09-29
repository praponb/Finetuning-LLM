from pyspark.sql import functions as F

prepared = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

joined = (
    prepared.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.opened_at") >= F.lit("2025-01-13 13:00:00").cast("timestamp")), "left")
    .select("s.*", F.col("t.closed_at").alias("ticket_closed_at"))
)

with_activity = (
    joined.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor").isNotNull()) & (F.col("t.event_type") == "resolved"), "left_anti")
)

result = (
    with_activity
    .groupingSets(
        [["ticket_id", "actor", "event_type"], ["ticket_id"]],
        "ticket_id", "actor", "event_type",
    )
    .agg(
        F.grouping("ticket_id").alias("is_total_ticket_id"),
        F.sum(F.when(F.col("actor") == "system", 1).otherwise(0)).alias("n_actor_system"),
        F.sum(F.when(F.col("actor") != "customer", 1).otherwise(0)).alias("n_actor_customer"),
    )
    .select(
        "ticket_id",
        F.coalesce(F.col("actor"), F.lit("ALL")).alias("actor"),
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        "is_total_ticket_id",
        "n_actor_system",
        "n_actor_customer",
    )
    .orderBy(
        F.col("ticket_id").asc_nulls_last(),
        F.col("actor").asc_nulls_last(),
        F.col("event_type").asc_nulls_last(),
    )
)
