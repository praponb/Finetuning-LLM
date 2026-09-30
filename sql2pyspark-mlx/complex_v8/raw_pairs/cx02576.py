from pyspark.sql import functions as F

joined_ranges = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 1 DAYS"))),
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

grouped = (
    joined_ranges
    .filter(F.col("agent_id") >= 3)
    .groupBy("priority", "agent_id")
    .agg(
        F.max("hours_since_open").alias("max_hours_since_open"),
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.round(100 * F.count(F.when(~F.col("priority").isin("P3", "P4"), 1)) / F.count("*"), 1).alias("pct_priority_not_in_p3_p4"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") >= 3)
)

result = (
    grouped
    .select(
        "priority",
        "agent_id",
        "max_hours_since_open",
        "pct_priority_not_in_p3_p4",
    )
    .unpivot(
        ids=["priority", "agent_id"],
        values=["max_hours_since_open", "pct_priority_not_in_p3_p4"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
