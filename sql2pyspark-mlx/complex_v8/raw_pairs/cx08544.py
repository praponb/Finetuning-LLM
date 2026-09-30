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
    .groupBy(F.to_date("event_at").alias("event_at_day"), "priority")
    .agg(
        F.sum("hours_since_open").alias("total_hours_since_open"),
        F.max("hours_since_open").alias("max_hours_since_open"),
    )
    .filter(F.col("total_hours_since_open") > 10.8)
)

scored = (
    grouped
    .select(
        "*",
        F.abs(F.col("total_hours_since_open") - 129.6).alias("total_hours_since_open_distance"),
        F.abs(F.col("max_hours_since_open") - 50.4).alias("max_hours_since_open_distance"),
        F.greatest(F.col("max_hours_since_open"), F.col("total_hours_since_open")).alias("greatest_max_hours_since_open_total_hours_since_"),
    )
)

annotated = (
    scored
    .filter(F.col("greatest_max_hours_since_open_total_hours_since_") > 43.2)
    .select(
        "*",
        F.when(F.col("priority").isin("P1", "P2"), "group_a").when(F.col("priority") == "P3", "group_b").otherwise("other").alias("priority_group"),
    )
)

result = (
    annotated
    .select(
        "event_at_day",
        "max_hours_since_open",
        "max_hours_since_open_distance",
        "greatest_max_hours_since_open_total_hours_since_",
        "priority_group",
        F.date_format(F.col("event_at_day"), "yyyy-MM-dd").alias("event_at_day_label"),
    )
    .orderBy(F.col("event_at_day").desc_nulls_first())
)
