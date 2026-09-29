from pyspark.sql import functions as F

base_events = (
    df_ticket_events
    .filter((F.col("event_at") >= F.lit("2025-01-25 13:00:00").cast("timestamp")) & (F.col("ticket_id") >= 51))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_activity_2 = (
    base_events.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.channel").isin("web", "email", "phone")), "left_semi")
)

result = (
    with_activity_2
    .rollup("event_type", "ticket_id", "actor")
    .agg(
        F.sum(F.when(F.col("actor").isin("customer", "system"), 1).otherwise(0)).alias("n_actor_in_customer_system"),
        F.count(F.when(~F.col("actor").isin("customer", "system"), 1)).alias("n_actor_not_in_customer_system"),
        F.count(F.when(F.col("event_type").isNotNull(), 1)).alias("n_event_type_is_not_null"),
    )
)
