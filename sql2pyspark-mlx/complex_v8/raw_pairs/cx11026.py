from pyspark.sql import functions as F

joined_ranges = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 3 DAYS"))),
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

summary = (
    joined_ranges
    .filter(F.col("ticket_id") >= 43)
    .groupBy("agent_id", "priority")
    .agg(
        F.sum(F.when(F.col("priority") == "P2", F.col("hours_since_open")).otherwise(0)).alias("hours_since_open_priority_p2"),
        F.min("hours_since_open").alias("min_hours_since_open"),
        F.count("*").alias("row_count"),
    )
)

result = (
    summary
    .select(
        "agent_id",
        "priority",
        "min_hours_since_open",
        "hours_since_open_priority_p2",
    )
    .unpivot(
        ids=["agent_id", "priority"],
        values=["min_hours_since_open", "hours_since_open_priority_p2"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
