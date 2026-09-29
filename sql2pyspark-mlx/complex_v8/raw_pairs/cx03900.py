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

rollup_base = (
    covered
    .filter(F.col("hours_since_open") > 28.8)
    .groupBy("priority", "agent_id", "event_type")
    .agg(
        F.round(F.avg("hours_since_open"), 2).alias("avg_hours_since_open"),
        F.round(F.stddev("hours_since_open"), 2).alias("stddev_hours_since_open"),
        F.count(F.when(F.col("priority") == "P4", 1)).alias("n_priority_p4"),
        F.min("event_at").alias("first_event_at"),
        F.count("*").alias("cnt"),
    )
)

result = (
    rollup_base
    .groupBy("n_priority_p4")
    .pivot("event_type", ["escalated", "assigned", "replied"])
    .agg(F.min("cnt").alias("min_v"), F.count("cnt").alias("n"))
    .orderBy("n_priority_p4")
)
