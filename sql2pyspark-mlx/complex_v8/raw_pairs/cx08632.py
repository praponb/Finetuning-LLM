from pyspark.sql import functions as F

covered = (
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
    covered
    .filter(F.month(F.col("event_at")).isin(2, 3))
    .groupBy("ticket_id")
    .agg(
        F.round(F.stddev("hours_since_open"), 2).alias("stddev_hours_since_open"),
        F.max("hours_since_open").alias("max_hours_since_open"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 2)
)

scored = (
    summary
    .filter(F.col("stddev_hours_since_open") < 64.8)
    .select(
        "ticket_id",
        "stddev_hours_since_open",
        "max_hours_since_open",
        F.round(F.col("stddev_hours_since_open") * 1.1, 2).alias("stddev_hours_since_open_adj"),
    )
)

result = (
    scored
    .select(
        "ticket_id",
        F.col("max_hours_since_open").alias("max_hours_since_open_v"),
        F.col("stddev_hours_since_open").alias("stddev_hours_since_open_v"),
    )
    .unpivot(
        ids=["ticket_id"],
        values=["max_hours_since_open_v", "stddev_hours_since_open_v"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("ticket_id", "metric_name")
)
