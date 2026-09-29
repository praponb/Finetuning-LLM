from pyspark.sql import functions as F

matched = (
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

labeled = (
    matched
    .filter(F.month(F.col("event_at")).isin(1, 2))
    .select(
        "*",
        F.concat(F.col("event_type"), F.lit("#"), F.col("id").cast("string")).alias("event_type_ref"),
    )
)

filtered = (
    labeled.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.to_date(F.col("t.event_at")) < F.lit("2025-03-02").cast("date")) & (F.col("t.event_type").isNotNull()), "left_anti")
)

result = (
    filtered
    .groupBy("ticket_id")
    .pivot("priority", ["P2", "P4"])
    .agg(F.min("hours_since_open"))
)
