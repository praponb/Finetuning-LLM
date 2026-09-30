from pyspark.sql import functions as F

in_window = (
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

grouped = (
    in_window
    .groupBy(
        F.date_format("event_at", "yyyy-MM").alias("event_at_ym"),
        "event_type",
        "ticket_id",
    )
    .agg(
        F.round(F.avg("hours_since_open"), 2).alias("avg_hours_since_open"),
        F.max("hours_since_open").alias("max_hours_since_open"),
        F.sum(F.when(F.col("priority").isin("P1", "P3", "P4"), F.col("hours_since_open")).otherwise(0)).alias("hours_since_open_priority_in_p1_p3_p4"),
        F.max("event_at").alias("last_event_at"),
        F.array_sort(F.collect_set("priority")).alias("priority_list"),
        F.sum("hours_since_open").alias("total_hours_since_open"),
    )
    .filter(F.col("total_hours_since_open") > 28.8)
)

without_matches = (
    grouped.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor").isNotNull()), "left_semi")
    .filter(F.col("hours_since_open_priority_in_p1_p3_p4") >= 126.0)
)

result = (
    without_matches
    .filter(F.col("avg_hours_since_open") >= 21.6)
    .select(
        "event_at_ym",
        "event_type",
        "avg_hours_since_open",
        "max_hours_since_open",
        "hours_since_open_priority_in_p1_p3_p4",
        "priority_list",
        "total_hours_since_open",
        F.abs(F.col("total_hours_since_open") - 280.8).alias("total_hours_since_open_distance"),
        F.when(F.col("avg_hours_since_open") >= 46.8, "HIGH").when(F.col("avg_hours_since_open") >= 25.2, "MEDIUM").otherwise("LOW").alias("avg_hours_since_open_tier"),
    )
    .orderBy("avg_hours_since_open")
)
