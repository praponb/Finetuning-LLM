from pyspark.sql import functions as F

cleaned_events = (
    df_ticket_events
    .filter(F.col("event_type").isin("created", "assigned"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("actor")), 1, 3).alias("actor_prefix"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(F.col("event_type") == "assigned", 1)).alias("event_event_type_count"),
        F.min("event_at").alias("first_event_at"),
    )
)

summarized_rows = (
    cleaned_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
        "t.first_event_at",
    )
)

event_stats_2 = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.min("event_at").alias("first_event_at_2"),
    )
)

with_stats = (
    summarized_rows.alias("s")
    .join(event_stats_2.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.last_event_at", "t.first_event_at_2")
)

result = (
    with_stats
    .select("id", "event_event_type_count", "days_since_event_at")
    .unpivot(
        ids=["id"],
        values=["event_event_type_count", "days_since_event_at"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "metric")
)
