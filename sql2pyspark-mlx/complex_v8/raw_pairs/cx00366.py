from pyspark.sql import functions as F

cleaned = (
    df_ticket_events
    .filter((F.col("ticket_id") >= 46) & (F.col("event_type") != "resolved"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws("-", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at_2"),
    )
)

qualified = (
    cleaned.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.hour(F.col("t.event_at")).between(10, 15)) & (~F.col("t.event_type").isin("assigned", "reopened")), "left_anti")
)

filtered = (
    qualified.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor").isin("agent", "system")) & (F.hour(F.col("t.event_at")).between(7, 9)), "left_anti")
)

with_details_ticket = (
    filtered.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.csat").alias("ticket_csat"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.count(F.when(F.col("actor") == "agent", 1)).alias("event_actor_count"),
    )
)

profiled = (
    with_details_ticket.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
    )
)

scored = (
    profiled
    .filter(F.col("ticket_csat") <= 4)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "event_type_actor_label",
        "days_since_event_at_2",
        "ticket_channel",
        "ticket_opened_at",
        "ticket_csat",
        F.greatest(F.col("event_actor_count"), F.col("days_since_event_at")).alias("greatest_event_actor_count_days_since_event_at"),
        F.lower(F.regexp_replace(F.trim(F.col("ticket_channel")), " ", "_")).alias("ticket_channel_slug"),
        (F.floor(F.col("days_since_event_at") / 100) * 100).alias("days_since_event_at_bucket"),
    )
)

result = (
    scored
    .select(
        "id",
        "ticket_id",
        "event_type",
        "event_type_actor_label",
        "days_since_event_at_2",
        "ticket_channel",
        "ticket_opened_at",
        "ticket_csat",
        "greatest_event_actor_count_days_since_event_at",
        "ticket_channel_slug",
        "days_since_event_at_bucket",
        F.year(F.col("ticket_opened_at")).alias("ticket_opened_at_year"),
        F.when(F.col("event_type").isin("created", "assigned"), "group_a").when(F.col("event_type") == "reopened", "group_b").otherwise("other").alias("event_type_group"),
    )
    .orderBy(F.col("days_since_event_at_bucket").desc_nulls_first())
)
