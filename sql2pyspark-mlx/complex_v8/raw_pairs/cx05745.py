from pyspark.sql import functions as F

cleaned_tickets = (
    df_tickets
    .filter((F.size(F.col("tags")) >= 2) | (F.col("requester_id") < 17))
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
        (F.col("closed_at") + F.expr("INTERVAL 7 DAYS")).alias("closed_at_plus_7d"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
        F.date_format(F.col("opened_at"), "yyyy-MM-dd").alias("opened_at_label"),
    )
)

combined_details = (
    cleaned_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.csat",
        "s.tags",
        "s.closed_at_plus_7d",
        "s.days_since_closed_at",
        "s.opened_at_label",
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
    )
)

detail_rows = (
    combined_details.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_type") == "replied"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.tags",
        "s.days_since_closed_at",
        "s.agent_hired_on",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        F.col("t.actor").alias("event_actor"),
        "t.event_type",
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(~F.col("event_type").isin("reopened", "replied"), 1)).alias("event_event_type_count"),
        F.min("event_at").alias("first_event_at"),
    )
)

summarized_rows = (
    detail_rows.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
        "t.first_event_at",
    )
)

combined_details_event = (
    summarized_rows.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.id") == F.col("s.event_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.tags",
        "s.days_since_closed_at",
        "s.agent_hired_on",
        "s.event_id",
        "s.event_at",
        "s.event_actor",
        "s.event_type",
        "s.event_event_type_count",
        "s.first_event_at",
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor_2"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

result = (
    combined_details_event
    .select("event_actor_2", "requester_id", "agent_hired_on")
    .distinct()
    .orderBy(
        F.col("agent_hired_on").desc_nulls_first(),
        F.col("requester_id").desc(),
    )
)
