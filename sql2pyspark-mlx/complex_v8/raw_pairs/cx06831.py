from pyspark.sql import functions as F

covered = (
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

rollup_base = (
    covered
    .groupBy("priority")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("event_at").alias("last_event_at"),
        F.percentile_approx("hours_since_open", 0.5).alias("median_hours_since_open"),
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.sum("hours_since_open").alias("total_hours_since_open"),
        F.sum(F.when(F.col("event_type") != "resolved", F.col("hours_since_open")).otherwise(0)).alias("hours_since_open_event_type_resolved"),
    )
    .filter(F.col("total_hours_since_open") > 21.6)
)

result = (
    rollup_base
    .filter(F.col("median_hours_since_open").between(18.0, 61.2))
    .select(
        "priority",
        "n_rows",
        "last_event_at",
        "median_hours_since_open",
        "total_hours_since_open",
        "hours_since_open_event_type_resolved",
        (F.col("median_hours_since_open") - F.col("n_rows")).alias("median_hours_since_open_minus_n_rows"),
    )
    .orderBy(
        F.col("priority").desc_nulls_first(),
        F.col("median_hours_since_open_minus_n_rows").desc(),
    )
)
