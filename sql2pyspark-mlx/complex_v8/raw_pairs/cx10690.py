from pyspark.sql import functions as F

recent = (
    df_tickets
    .filter((F.col("channel") == "phone") & (~F.array_contains(F.col("tags"), "outage")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "tags",
        F.when(F.col("priority").isin("P2", "P3"), "group_a").when(F.col("priority") == "P1", "group_b").otherwise("other").alias("priority_group"),
        F.date_trunc("day", F.col("closed_at")).alias("closed_at_day"),
        F.round(F.col("csat") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("csat_per_requester_id"),
    )
)

without_matches = (
    recent
    .crossJoin(recent.agg(F.avg("csat_per_requester_id").alias("overall_avg_csat_per_requester_id")))
    .filter(F.col("csat_per_requester_id") > F.col("overall_avg_csat_per_requester_id"))
    .drop("overall_avg_csat_per_requester_id")
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.count("*").alias("event_count"),
    )
)

profiled = (
    without_matches.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.first_event_at",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

result = (
    profiled
    .select("id", "event_count", "requester_id")
    .unpivot(
        ids=["id"],
        values=["event_count", "requester_id"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
