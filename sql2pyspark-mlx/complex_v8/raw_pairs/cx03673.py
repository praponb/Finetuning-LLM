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

rollup_base = (
    joined_ranges
    .groupBy(
        "ticket_id",
        F.when(F.col("hours_since_open") >= 43.2, "A").when(F.col("hours_since_open") >= 28.8, "B").otherwise("C").alias("hours_since_open_tier"),
        "event_type",
    )
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.round(F.sum("hours_since_open"), 2).alias("total_hours_since_open"),
        F.round(F.avg("hours_since_open"), 2).alias("avg_hours_since_open"),
        F.count(F.when(~F.col("priority").isin("P3", "P2"), 1)).alias("n_priority_not_in_p3_p2"),
        F.countDistinct("agent_id").alias("distinct_agent_ids"),
        F.sum("hours_since_open").alias("total_hours_since_open_2"),
    )
)

annotated = (
    rollup_base
    .select(
        "ticket_id",
        "hours_since_open_tier",
        "event_type",
        "first_event_at",
        "total_hours_since_open",
        "n_priority_not_in_p3_p2",
        "distinct_agent_ids",
        "total_hours_since_open_2",
        F.dayofweek(F.col("first_event_at")).alias("first_event_at_dayofweek"),
        F.coalesce(F.col("total_hours_since_open"), F.lit(0.0)).alias("total_hours_since_open_filled"),
    )
)

result = (
    annotated
    .filter(F.col("hours_since_open_tier") == "A")
    .select(
        "ticket_id",
        "hours_since_open_tier",
        "event_type",
        "first_event_at",
        "total_hours_since_open",
        "n_priority_not_in_p3_p2",
        "total_hours_since_open_2",
        "first_event_at_dayofweek",
        "total_hours_since_open_filled",
        F.round(F.col("total_hours_since_open") * 1.5, 2).alias("total_hours_since_open_adj"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_type_ref"),
    )
    .orderBy(
        F.col("first_event_at").desc(),
        "ticket_id",
        "hours_since_open_tier",
        "event_type",
    )
    .limit(20)
)
