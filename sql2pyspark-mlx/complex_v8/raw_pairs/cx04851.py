from pyspark.sql import functions as F

cleaned_tickets = (
    df_tickets
    .filter(F.col("agent_id").isNotNull())
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("actor").isin("agent", "customer"), 1)).alias("event_actor_count"),
    )
)

with_counts = (
    cleaned_tickets.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
    )
)

event_stats_2 = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.max("event_at").alias("last_event_at_2"))
)

summarized_rows = (
    with_counts.alias("s")
    .join(event_stats_2.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select("s.*", "t.last_event_at_2")
)

result = (
    summarized_rows
    .select(
        "id",
        "event_count",
        "csat",
        "event_actor_count",
    )
    .unpivot(
        ids=["id"],
        values=["event_count", "csat", "event_actor_count"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "measure")
)
