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

priority_stats = (
    covered
    .groupBy("priority")
    .agg(F.avg("hours_since_open").alias("avg_hours_since_open"))
)

vs_group = (
    covered.alias("s")
    .join(priority_stats.alias("g"), F.col("g.priority") == F.col("s.priority"), "inner")
    .filter(F.col("s.hours_since_open") > F.col("g.avg_hours_since_open"))
    .select(
        "s.*",
        "g.avg_hours_since_open",
        F.round(F.col("s.hours_since_open") - F.col("g.avg_hours_since_open"), 2).alias("hours_since_open_minus_avg"),
    )
)

result = (
    vs_group
    .filter(F.col("avg_hours_since_open").between(3.6, 50.4))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "priority",
        "agent_id",
        "hours_since_open",
        "avg_hours_since_open",
        "hours_since_open_minus_avg",
        F.lower(F.trim(F.col("event_type"))).alias("event_type_clean"),
        F.concat_ws("|", F.col("priority"), F.col("event_type")).alias("priority_event_type_label"),
    )
    .orderBy(
        F.col("event_type_clean").asc_nulls_last(),
        F.col("ticket_id").desc_nulls_first(),
    )
)
