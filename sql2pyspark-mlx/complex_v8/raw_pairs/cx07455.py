from pyspark.sql import functions as F

joined_ranges = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 7 DAYS"))),
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

scored = (
    joined_ranges
    .filter(F.col("hours_since_open") > 18.0)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "priority",
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
        F.coalesce(F.col("hours_since_open"), F.lit(0.0)).alias("hours_since_open_filled"),
        (F.floor(F.col("hours_since_open") / 10) * 10).alias("hours_since_open_bucket"),
    )
)

result = (
    scored
    .filter(F.col("hours_since_open_bucket") > 14)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "priority",
        "priority_slug",
        "hours_since_open_filled",
        "hours_since_open_bucket",
        F.coalesce(F.col("hours_since_open_filled"), F.lit(0.0)).alias("hours_since_open_filled_filled"),
    )
    .orderBy(F.col("id").asc_nulls_last(), F.col("event_at").desc())
    .limit(20)
)
