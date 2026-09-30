from pyspark.sql import functions as F

joined_ranges = (
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

qualified = (
    joined_ranges.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type").isin("replied", "escalated", "reopened")) & (F.col("t.event_at") < F.lit("2025-03-08 09:00:00").cast("timestamp")), "left_anti")
    .filter(F.col("agent_id").isNotNull())
)

labeled = (
    qualified
    .select(
        "id",
        "event_at",
        "event_type",
        "priority",
        "agent_id",
        F.upper(F.trim(F.col("event_type"))).alias("event_type_clean"),
        F.round(F.col("hours_since_open") * 1.07, 2).alias("hours_since_open_adj"),
    )
)

result = (
    labeled
    .select(
        "id",
        "event_at",
        "event_type",
        "priority",
        "agent_id",
        "hours_since_open_adj",
        F.substring(F.trim(F.col("priority")), 1, 4).alias("priority_prefix"),
        F.substring(F.trim(F.col("event_type_clean")), 1, 2).alias("event_type_clean_prefix"),
    )
    .orderBy(F.col("hours_since_open_adj").desc())
)
