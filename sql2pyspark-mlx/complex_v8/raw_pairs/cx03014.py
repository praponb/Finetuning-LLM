from pyspark.sql import functions as F

recent_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("actor"), F.lit("#"), F.col("id").cast("string")).alias("actor_ref"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("id").cast("string")).alias("event_type_ref"),
    )
)

with_activity = (
    recent_events.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (~F.col("t.event_type").isin("replied", "assigned")) & (F.col("t.event_at") >= F.lit("2025-01-13 11:00:00").cast("timestamp")), "left_anti")
)

joined = (
    with_activity.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.actor_ref",
        "s.event_type_ref",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

line_level = (
    joined.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.ticket_agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.actor",
        "s.event_type_ref",
        "s.ticket_agent_id",
        "s.ticket_channel",
        F.col("t.id").alias("ticket_id_2"),
        F.col("t.closed_at").alias("ticket_closed_at_2"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

rollup_base = (
    line_level
    .groupBy("actor")
    .agg(
        F.array_sort(F.collect_set("ticket_channel")).alias("ticket_channel_list"),
        F.min("ticket_opened_at").alias("first_ticket_opened_at"),
        F.max("ticket_opened_at").alias("last_ticket_opened_at"),
        F.avg("ticket_csat").alias("avg_ticket_csat_check"),
    )
    .filter(F.col("avg_ticket_csat_check") > 1)
    .drop("avg_ticket_csat_check")
)

enriched = (
    rollup_base
    .select(
        "*",
        F.date_format(F.col("first_ticket_opened_at"), "yyyy-MM-dd").alias("first_ticket_opened_at_label"),
        F.size(F.col("ticket_channel_list")).alias("ticket_channel_list_count"),
    )
)

per_label = (
    enriched
    .select(
        "actor",
        "first_ticket_opened_at",
        "last_ticket_opened_at",
        "first_ticket_opened_at_label",
        "ticket_channel_list_count",
        F.explode("ticket_channel_list").alias("ticket_channel_list_item"),
    )
)

result = (
    per_label
    .select(
        "actor",
        "first_ticket_opened_at",
        "last_ticket_opened_at",
        "first_ticket_opened_at_label",
        "ticket_channel_list_count",
        "ticket_channel_list_item",
        F.substring(F.trim(F.col("actor")), 1, 4).alias("actor_prefix"),
        F.length(F.trim(F.col("ticket_channel_list_item"))).alias("ticket_channel_list_item_len"),
    )
)
