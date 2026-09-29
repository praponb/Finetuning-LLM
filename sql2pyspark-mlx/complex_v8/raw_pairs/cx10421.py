from pyspark.sql import functions as F

recent = (
    df_tickets
    .filter((F.hour(F.col("opened_at")).between(6, 10)) & (F.col("requester_id") <= 18))
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
        F.concat_ws(": ", F.col("priority"), F.col("channel")).alias("priority_channel_label"),
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.sum("csat").alias("total_ticket_csat"),
        F.max("closed_at").alias("last_ticket_closed_at"),
        F.max("closed_at").alias("last_ticket_closed_at_2"),
    )
)

summarized_rows = (
    recent.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "t.total_ticket_csat",
        "t.last_ticket_closed_at",
        "t.last_ticket_closed_at_2",
    )
)

exploded_detail = (
    summarized_rows.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.channel_len",
        "s.total_ticket_csat",
        "s.last_ticket_closed_at_2",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        F.col("t.actor").alias("event_actor"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(~F.col("actor").isin("customer", "agent"), 1)).alias("event_actor_count"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("actor") != "agent", 1)).alias("event_actor_count_2"),
    )
)

summarized_rows_2 = (
    exploded_detail.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        F.coalesce(F.col("t.event_actor_count_2"), F.lit(0)).alias("event_actor_count_2"),
    )
)

result = (
    summarized_rows_2
    .select("channel")
    .distinct()
    .orderBy(F.col("channel").desc_nulls_first())
)
