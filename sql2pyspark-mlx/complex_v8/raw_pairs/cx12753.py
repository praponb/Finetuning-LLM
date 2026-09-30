from pyspark.sql import functions as F

valid = (
    df_tickets
    .filter(F.array_contains(F.col("tags"), "login"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "csat",
        "tags",
    )
)

line_level_events = (
    valid.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_at") >= F.lit("2025-02-06 10:00:00").cast("timestamp")), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        "t.event_at",
        F.col("t.actor").alias("event_actor"),
    )
)

combined_details = (
    line_level_events.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.id") == F.col("s.event_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.event_id",
        "s.event_at",
        "s.event_actor",
        F.col("t.event_at").alias("event_event_at"),
    )
)

labeled = (
    combined_details
    .filter(F.col("event_at") >= F.lit("2025-01-25 14:00:00").cast("timestamp"))
    .withColumn("days_since_event_event_at", F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_event_at")))
)

event_actor_stats = (
    labeled
    .groupBy("event_actor")
    .agg(F.avg("requester_id").alias("avg_requester_id"))
)

outliers = (
    labeled.alias("s")
    .join(event_actor_stats.alias("g"), F.col("g.event_actor") == F.col("s.event_actor"), "inner")
    .filter(F.col("s.requester_id") > F.col("g.avg_requester_id"))
    .select(
        "s.*",
        "g.avg_requester_id",
        F.round(F.col("s.requester_id") - F.col("g.avg_requester_id"), 2).alias("requester_id_minus_avg"),
    )
)

totals = (
    outliers
    .groupBy("agent_id", F.date_trunc("month", "event_at").alias("event_at_month"))
    .agg(
        F.count(F.when(F.col("event_actor") == "customer", 1)).alias("n_event_actor_customer"),
        F.array_sort(F.collect_set("event_actor")).alias("event_actor_list"),
        F.min("opened_at").alias("first_opened_at"),
        F.count("*").alias("n_rows"),
        F.min("avg_requester_id").alias("min_avg_requester_id"),
    )
)

with_details_agent = (
    totals.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.agent_id",
        "s.event_at_month",
        "s.n_event_actor_customer",
        "s.n_rows",
        "s.min_avg_requester_id",
        F.col("su.team").alias("agent_team"),
    )
)

result = (
    with_details_agent
    .filter(F.month(F.col("event_at_month")).isin(2, 3))
    .select(
        "agent_id",
        "event_at_month",
        "n_event_actor_customer",
        "n_rows",
        "agent_team",
    )
    .orderBy(F.col("event_at_month").asc())
)
