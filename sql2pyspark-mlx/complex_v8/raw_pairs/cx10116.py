from pyspark.sql import functions as F

prepared_tickets = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "closed_at",
        "channel",
        "csat",
        F.date_format(F.col("closed_at"), "yyyy-MM-dd").alias("closed_at_label"),
        F.array_join(F.array_sort(F.col("tags")), ",").alias("tags_text"),
        F.abs(F.col("requester_id") - 18).alias("requester_id_distance"),
    )
)

enriched_agent = (
    prepared_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.channel",
        "s.csat",
        "s.closed_at_label",
        "s.tags_text",
        "s.requester_id_distance",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

enriched = (
    enriched_agent
    .select(
        "id",
        "agent_id",
        "closed_at",
        "channel",
        "tags_text",
        "agent_hired_on",
        F.abs(F.col("csat") - 4).alias("csat_distance"),
        F.when(F.col("requester_id_distance") >= 15, "A").when(F.col("requester_id_distance") >= 6, "B").otherwise("C").alias("requester_id_distance_tier"),
    )
)

line_level_events = (
    enriched.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.channel",
        "s.tags_text",
        "s.agent_hired_on",
        "s.requester_id_distance_tier",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        "t.event_at",
    )
)

result = (
    line_level_events
    .groupingSets(
        [["channel", "event_id"], ["channel"], ["event_id"], []],
        "channel", "event_id",
    )
    .agg(F.countDistinct("channel").alias("distinct_channels"))
    .orderBy(F.col("channel").asc_nulls_last(), F.col("event_id").asc_nulls_last())
)
