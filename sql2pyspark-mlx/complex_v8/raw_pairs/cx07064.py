from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("agent_id", "priority_or_unknown").orderBy(F.col("avg_csat").desc(), F.col("id").desc(), F.col("event_id").desc())

cleaned_tickets = (
    df_tickets
    .filter(F.col("closed_at") < F.lit("2025-02-22 12:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
        F.date_format(F.col("opened_at"), "yyyy-MM").alias("opened_at_label"),
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
    )
)

enriched = (
    cleaned_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.opened_at_label",
        "s.priority_or_unknown",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

detail_rows_events = (
    enriched.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.channel",
        "s.csat",
        "s.opened_at_label",
        "s.priority_or_unknown",
        "s.agent_team",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        F.col("t.actor").alias("event_actor"),
    )
)

event_actor_stats = (
    detail_rows_events
    .groupBy("event_actor")
    .agg(F.avg("csat").alias("avg_csat"))
)

outliers = (
    detail_rows_events.alias("s")
    .join(event_actor_stats.alias("g"), F.col("g.event_actor") == F.col("s.event_actor"), "inner")
    .filter(F.col("s.csat") > F.col("g.avg_csat"))
    .select(
        "s.*",
        "g.avg_csat",
        F.round(F.col("s.csat") - F.col("g.avg_csat"), 2).alias("csat_minus_avg"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count("*").alias("ticket_count"),
        F.count(F.when(F.col("priority") == "P4", 1)).alias("ticket_priority_count"),
    )
)

with_stats = (
    outliers.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        F.coalesce(F.col("t.ticket_priority_count"), F.lit(0)).alias("ticket_priority_count"),
    )
)

deduped = (
    with_stats
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    deduped
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "channel",
        "csat",
        "priority_or_unknown",
        "agent_team",
        "event_id",
        "event_type",
        "event_actor",
        "avg_csat",
        "csat_minus_avg",
        F.lower(F.trim(F.col("event_actor"))).alias("event_actor_clean"),
        F.concat(F.col("channel"), F.lit("#"), F.col("id").cast("string")).alias("channel_ref"),
    )
    .orderBy("id")
)
