from pyspark.sql import functions as F

matched = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 2 DAYS"))),
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

per_group = (
    matched
    .groupBy("priority")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.max("event_at").alias("last_event_at"),
        F.percentile_approx("hours_since_open", 0.5).alias("median_hours_since_open"),
        F.count("*").alias("cnt"),
        F.round(F.avg("hours_since_open"), 2).alias("avg_hours_since_open"),
    )
)

result = (
    per_group
    .select("priority", "avg_hours_since_open", "median_hours_since_open")
    .unpivot(
        ids=["priority"],
        values=["avg_hours_since_open", "median_hours_since_open"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
