from pyspark.sql import functions as F

joined_ranges = (
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

with_children_events = (
    joined_ranges.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.priority",
        "s.agent_id",
        "s.hours_since_open",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

result = (
    with_children_events
    .groupingSets(
        [["ticket_id", "agent_id"], ["ticket_id"], ["agent_id"]],
        "ticket_id", "agent_id",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.min("event_event_at").alias("first_event_event_at"),
        F.max("event_event_at").alias("last_event_event_at"),
        F.sum(F.when(F.col("event_type").isin("replied", "created"), 1).otherwise(0)).alias("n_event_type_in_replied_create"),
    )
    .orderBy(F.col("ticket_id").asc_nulls_last(), F.col("agent_id").asc_nulls_last())
)
