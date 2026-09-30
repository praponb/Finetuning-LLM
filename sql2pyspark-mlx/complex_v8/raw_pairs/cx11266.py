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

per_group = (
    joined_ranges
    .groupBy("ticket_id", "event_type")
    .agg(
        F.min("hours_since_open").alias("min_hours_since_open"),
        F.round(F.sum("hours_since_open"), 2).alias("total_hours_since_open"),
        F.count(F.when(F.col("event_type") == "replied", 1)).alias("n_event_type_replied"),
        F.max("hours_since_open").alias("max_hours_since_open"),
        F.count("*").alias("cnt"),
    )
)

with_activity = (
    per_group
    .crossJoin(per_group.agg(F.min("min_hours_since_open").alias("overall_min_min_hours_since_open")))
    .filter(F.col("min_hours_since_open") > F.col("overall_min_min_hours_since_open"))
    .drop("overall_min_min_hours_since_open")
)

result = (
    with_activity
    .filter(F.col("cnt") <= 10)
    .select(
        "ticket_id",
        "event_type",
        "total_hours_since_open",
        "n_event_type_replied",
        "cnt",
        F.coalesce(F.col("cnt"), F.lit(0)).alias("cnt_filled"),
        F.round(F.col("total_hours_since_open") * 1.07, 2).alias("total_hours_since_open_adj"),
    )
    .orderBy(
        F.col("total_hours_since_open_adj").desc(),
        F.col("n_event_type_replied").desc_nulls_first(),
    )
)
