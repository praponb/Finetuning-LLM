from pyspark.sql import functions as F

filtered = (
    df_ticket_events
    .filter(~F.col("event_type").isin("reopened", "resolved"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_parent = (
    filtered.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.actor",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
    )
)

with_activity = (
    with_parent.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor") == "system"), "left_anti")
)

filtered_2 = (
    with_activity.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left_semi")
)

enriched = (
    filtered_2
    .select(
        "*",
        F.abs(F.col("ticket_requester_id") - 18).alias("ticket_requester_id_distance"),
        F.lower(F.regexp_replace(F.trim(F.col("ticket_channel")), " ", "_")).alias("ticket_channel_slug"),
        F.concat(F.col("ticket_channel"), F.lit("#"), F.col("id").cast("string")).alias("ticket_channel_ref"),
    )
)

qualified = (
    enriched.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left_semi")
)

detail_rows = (
    qualified.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.agent_name",
        "s.ticket_channel_ref",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        "t.event_at",
        F.col("t.actor").alias("event_actor"),
    )
)

result = (
    detail_rows
    .groupingSets(
        [["event_actor", "event_id"], ["event_actor"], []],
        "event_actor", "event_id",
    )
    .agg(
        F.grouping("event_actor").alias("is_total_event_actor"),
        F.count("*").alias("row_count"),
    )
    .orderBy(
        F.col("event_actor").asc_nulls_last(),
        F.col("event_id").asc_nulls_last(),
    )
)
