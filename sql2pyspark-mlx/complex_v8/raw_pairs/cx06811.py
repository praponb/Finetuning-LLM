from pyspark.sql import functions as F

scoped_events = (
    df_ticket_events
    .filter((F.to_date(F.col("event_at")) < F.lit("2025-02-06").cast("date")) & (F.col("event_type") == "created"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("actor")), 1, 3).alias("actor_prefix"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(F.col("event_type") != "resolved", 1)).alias("event_event_type_count"),
        F.max("event_at").alias("last_event_at"),
        F.max("event_at").alias("last_event_at_2"),
    )
)

with_counts = (
    scoped_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
        "t.last_event_at",
        "t.last_event_at_2",
    )
)

combined_details = (
    with_counts.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.*",
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

result = (
    combined_details
    .filter(F.col("event_event_type_count") < 4)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_type_or_unknown",
        "event_event_type_count",
        "last_event_at_2",
        "ticket_tags",
        "ticket_opened_at",
        "agent_hired_on",
        F.coalesce(F.col("event_event_type_count"), F.lit(0)).alias("event_event_type_count_filled"),
        F.abs(F.col("event_event_type_count") - 2).alias("event_event_type_count_distance"),
    )
    .orderBy(F.col("event_at").desc_nulls_first(), "actor")
)
