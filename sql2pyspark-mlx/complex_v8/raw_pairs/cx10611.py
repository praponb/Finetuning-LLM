from pyspark.sql import functions as F

scoped = (
    df_tickets
    .filter((F.size(F.col("tags")) >= 1) & (F.col("csat") >= 3))
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

combined_details = (
    scoped.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (~F.col("su.tier").isin("T1", "T2")), "inner")
    .filter(F.col("su.tier").isNotNull())
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.tags",
        F.col("su.tier").alias("agent_tier"),
    )
)

screened = (
    combined_details
    .crossJoin(combined_details.agg(F.avg("csat").alias("overall_avg_csat")))
    .filter(F.col("csat") > F.col("overall_avg_csat"))
    .drop("overall_avg_csat")
)

line_level = (
    screened.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.channel",
        "s.tags",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        "t.event_type",
        F.col("t.actor").alias("event_actor"),
    )
)

per_label = (
    line_level
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "event_id",
        "event_at",
        "event_type",
        "event_actor",
        F.explode_outer("tags").alias("tag"),
    )
)

filtered = (
    per_label.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.actor") == "system") & (F.col("t.event_type") == "created"), "left_anti")
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.min("opened_at").alias("first_ticket_opened_at"))
)

summarized_rows = (
    filtered.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select("s.*", "t.first_ticket_opened_at")
)

result = (
    summarized_rows
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "event_id",
        "event_at",
        "event_type",
        "tag",
        (F.floor(F.col("requester_id") / 5) * 5).alias("requester_id_bucket"),
    )
    .orderBy(F.col("requester_id_bucket").asc(), F.col("event_id").desc())
)
