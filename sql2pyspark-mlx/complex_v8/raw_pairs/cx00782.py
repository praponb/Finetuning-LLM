from pyspark.sql import functions as F

filtered = (
    df_ticket_events
    .filter((F.col("actor").isin("agent", "customer")) & (F.col("event_type").isin("created", "assigned", "reopened")))
)

detail_rows_events = (
    filtered.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.count("*").alias("event_count"),
    )
)

summarized_rows = (
    detail_rows_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.first_event_at",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

combined_details = (
    summarized_rows.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_ticket_events.alias("ti"), F.col("ti.id") == F.col("s.event_id"), "left")
    .filter(F.col("t.agent_id").isNotNull())
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_id",
        "s.event_actor",
        "s.event_event_type",
        "s.event_count",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("ti.event_at").alias("event_event_at"),
        F.col("ti.actor").alias("event_actor_2"),
    )
)

result = (
    combined_details
    .groupingSets(
        [["event_actor_2", "event_id"], ["event_actor_2"]],
        "event_actor_2", "event_id",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.sum(F.when(F.col("event_actor") != "system", F.col("event_count"))).alias("event_count_event_actor_system"),
    )
    .select(
        F.coalesce(F.col("event_actor_2"), F.lit("ALL")).alias("event_actor_2"),
        "event_id",
        "grouping_level",
        "event_count_event_actor_system",
    )
)
