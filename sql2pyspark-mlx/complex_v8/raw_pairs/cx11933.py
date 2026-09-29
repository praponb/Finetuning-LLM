from pyspark.sql import functions as F

base = (
    df_tickets
    .filter(F.col("opened_at") < F.lit("2025-02-24 15:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "csat",
        (F.floor(F.col("requester_id") / 5) * 5).alias("requester_id_bucket"),
        F.date_format(F.col("opened_at"), "yyyy-MM").alias("opened_at_label"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
    )
)

agent_id_stats = (
    base
    .groupBy("agent_id")
    .agg(F.avg("days_since_opened_at").alias("avg_days_since_opened_at"))
)

outliers = (
    base.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.days_since_opened_at") > F.col("g.avg_days_since_opened_at"))
    .select(
        "s.*",
        "g.avg_days_since_opened_at",
        F.round(F.col("s.days_since_opened_at") - F.col("g.avg_days_since_opened_at"), 2).alias("days_since_opened_at_minus_avg"),
    )
)

detail_rows_events = (
    outliers.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.actor") == "agent"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.priority",
        "s.csat",
        "s.requester_id_bucket",
        "s.days_since_opened_at",
        "s.avg_days_since_opened_at",
        "s.days_since_opened_at_minus_avg",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_type",
    )
)

result = (
    detail_rows_events
    .groupingSets(
        [["csat", "event_id", "event_type"], ["csat"], ["event_id"]],
        "csat", "event_id", "event_type",
    )
    .agg(F.count("*").alias("n_rows"))
    .select(
        "csat",
        "event_id",
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        "n_rows",
    )
    .orderBy(
        F.col("csat").asc_nulls_last(),
        F.col("event_id").asc_nulls_last(),
        F.col("event_type").asc_nulls_last(),
    )
)
