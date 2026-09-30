from pyspark.sql import functions as F

covered = (
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

agent_id_stats = (
    covered
    .groupBy("agent_id")
    .agg(F.percentile_approx("hours_since_open", 0.5).alias("median_hours_since_open"))
)

vs_group = (
    covered.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.hours_since_open") >= F.col("g.median_hours_since_open"))
    .select(
        "s.*",
        "g.median_hours_since_open",
        F.round(F.col("s.hours_since_open") - F.col("g.median_hours_since_open"), 2).alias("hours_since_open_minus_median"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.count("*").alias("event_count"))
)

summarized_rows = (
    vs_group.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

result = (
    summarized_rows
    .select(
        "id",
        "ticket_id",
        "priority",
        "agent_id",
        "hours_since_open",
        "hours_since_open_minus_median",
    )
    .orderBy(F.col("hours_since_open").desc())
)
