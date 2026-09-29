from pyspark.sql import functions as F

matched = (
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

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count("*").alias("ticket_count"),
        F.max("csat").alias("max_ticket_csat"),
        F.min("requester_id").alias("min_ticket_requester_id"),
    )
)

summarized_rows = (
    matched.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        "t.max_ticket_csat",
        "t.min_ticket_requester_id",
    )
)

with_children_events = (
    summarized_rows.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.priority",
        "s.agent_id",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.actor").alias("event_actor"),
    )
)

rollup_base = (
    with_children_events
    .groupBy("agent_id", F.date_trunc("month", "event_at").alias("event_at_month"))
    .agg(
        F.countDistinct("agent_id").alias("distinct_agent_ids"),
        F.round(100 * F.count(F.when(F.col("event_actor") != "customer", 1)) / F.count("*"), 1).alias("pct_event_actor_customer"),
        F.count(F.when(F.col("event_event_type").isin("escalated", "created", "resolved"), 1)).alias("n_event_event_type_in_escalate"),
        F.sum(F.when(F.col("priority") != "P2", 1).otherwise(0)).alias("n_priority_p2"),
        F.count(F.when(~F.col("event_event_type").isin("created", "resolved"), 1)).alias("n_event_event_type_not_in_crea"),
        F.round(100 * F.count(F.when(F.col("priority") != "P4", 1)) / F.count("*"), 1).alias("pct_priority_p4"),
    )
)

result = (
    rollup_base
    .filter(F.col("n_event_event_type_in_escalate") >= 2)
    .select(
        "agent_id",
        "distinct_agent_ids",
        "pct_event_actor_customer",
        "n_event_event_type_in_escalate",
        "n_priority_p2",
        "n_event_event_type_not_in_crea",
        F.date_trunc("month", F.col("event_at_month")).alias("event_at_month_month"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at_month")).alias("days_since_event_at_month"),
    )
    .orderBy(
        F.col("agent_id").desc(),
        F.col("days_since_event_at_month").desc_nulls_first(),
    )
)
