from pyspark.sql import functions as F

recent_events = (
    df_ticket_events
    .filter((F.col("event_at") < F.lit("2025-02-12 09:00:00").cast("timestamp")) | (F.col("event_type") == "escalated"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("year", F.col("event_at")).alias("event_at_year"),
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label"),
    )
)

joined = (
    recent_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.csat").alias("ticket_csat"),
    )
)

totals = (
    joined
    .groupBy(
        F.date_trunc("month", "event_at").alias("event_at_month"),
        "ticket_agent_id",
        "ticket_id",
    )
    .agg(
        F.sum(F.when(F.col("ticket_priority") == "P2", 1).otherwise(0)).alias("n_ticket_priority_p2"),
        F.round(F.stddev("ticket_csat"), 2).alias("stddev_ticket_csat"),
        F.array_sort(F.collect_set("ticket_priority")).alias("ticket_priority_list"),
        F.min("ticket_csat").alias("min_ticket_csat"),
        F.sum("ticket_csat").alias("total_ticket_csat"),
    )
)

with_parent_agent = (
    totals.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.ticket_agent_id"), "inner")
    .select(
        "s.event_at_month",
        "s.ticket_agent_id",
        "s.ticket_id",
        "s.n_ticket_priority_p2",
        "s.stddev_ticket_csat",
        "s.ticket_priority_list",
        "s.min_ticket_csat",
        "s.total_ticket_csat",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
    )
)

result = (
    with_parent_agent
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "event_at_month",
        "ticket_agent_id",
        "ticket_id",
        "n_ticket_priority_p2",
        "stddev_ticket_csat",
        "min_ticket_csat",
        "total_ticket_csat",
        "agent_team",
        "agent_hired_on",
        "agent_tier",
        F.when(F.col("stddev_ticket_csat") >= 3.2, "A").when(F.col("stddev_ticket_csat") >= 2.2, "B").otherwise("C").alias("stddev_ticket_csat_tier"),
    )
    .orderBy("agent_hired_on")
)
