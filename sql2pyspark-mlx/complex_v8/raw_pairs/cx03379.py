from pyspark.sql import functions as F

in_window = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 7 DAYS"))),
        "inner",
    )
    .select(
        "e.id",
        "e.ticket_id",
        "e.event_at",
        "e.event_type",
        "t.priority",
        "t.agent_id",
        F.round((F.unix_timestamp("e.event_at") - F.unix_timestamp("t.opened_at")) / 3600, 2).alias("hours_since_open"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(F.col("actor") == "system", 1)).alias("event_actor_count"),
        F.max("event_at").alias("last_event_at"),
    )
)

with_stats = (
    in_window.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
        "t.last_event_at",
    )
)

result = (
    with_stats
    .rollup("agent_id", "priority", "event_type")
    .agg(F.max("event_at").alias("last_event_at_2"))
    .select(
        "agent_id",
        F.coalesce(F.col("priority"), F.lit("ALL")).alias("priority"),
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        "last_event_at_2",
    )
    .orderBy(
        F.col("agent_id").asc_nulls_last(),
        F.col("priority").asc_nulls_last(),
        F.col("event_type").asc_nulls_last(),
    )
)
