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

tagged = (
    covered
    .select(
        "*",
        F.when(F.col("priority").isin("P2", "P1"), "group_a").when(F.col("priority") == "P3", "group_b").otherwise("other").alias("priority_group"),
        F.coalesce(F.col("hours_since_open"), F.lit(0.0)).alias("hours_since_open_filled"),
    )
)

result = (
    tagged
    .select(
        "id",
        "ticket_id",
        "event_at",
        "agent_id",
        "hours_since_open",
        "hours_since_open_filled",
    )
    .orderBy(F.col("id").desc_nulls_first())
    .limit(5)
)
