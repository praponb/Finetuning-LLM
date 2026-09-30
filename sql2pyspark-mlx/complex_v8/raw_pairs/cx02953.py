from pyspark.sql import functions as F

recent_tickets = (
    df_tickets
    .filter((F.col("csat").between(1, 4)) & (F.col("priority") == "P1"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
    )
)

priority_stats = (
    recent_tickets
    .groupBy("priority")
    .agg(F.percentile_approx("channel_len", 0.5).alias("median_channel_len"))
)

above_average = (
    recent_tickets.alias("s")
    .join(priority_stats.alias("g"), F.col("g.priority") == F.col("s.priority"), "inner")
    .filter(F.col("s.channel_len") >= F.col("g.median_channel_len"))
    .select(
        "s.*",
        "g.median_channel_len",
        F.round(F.col("s.channel_len") - F.col("g.median_channel_len"), 2).alias("channel_len_minus_median"),
    )
)

priority_stats_2 = (
    above_average
    .groupBy("priority")
    .agg(F.percentile_approx("median_channel_len", 0.5).alias("median_median_channel_len"))
)

vs_group = (
    above_average.alias("s")
    .join(priority_stats_2.alias("g"), F.col("g.priority") == F.col("s.priority"), "inner")
    .filter(F.col("s.median_channel_len") >= F.col("g.median_median_channel_len"))
    .select(
        "s.*",
        "g.median_median_channel_len",
        F.round(F.col("s.median_channel_len") - F.col("g.median_median_channel_len"), 2).alias("median_channel_len_minus_median"),
    )
)

joined = (
    vs_group.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.priority",
        "s.channel",
        "s.channel_len",
        "s.median_channel_len",
        "s.channel_len_minus_median",
        "s.median_median_channel_len",
        "s.median_channel_len_minus_median",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
    )
)

detail_rows = (
    joined.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.channel_len",
        "s.channel_len_minus_median",
        "s.median_median_channel_len",
        "s.agent_hired_on",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_type",
        "t.event_at",
    )
)

event_actor_stats = (
    detail_rows
    .groupBy("event_actor")
    .agg(F.avg("channel_len").alias("avg_channel_len"))
)

vs_group_2 = (
    detail_rows.alias("s")
    .join(event_actor_stats.alias("g"), F.col("g.event_actor") == F.col("s.event_actor"), "inner")
    .filter(F.col("s.channel_len") > F.col("g.avg_channel_len"))
    .select(
        "s.*",
        "g.avg_channel_len",
        F.round(F.col("s.channel_len") - F.col("g.avg_channel_len"), 2).alias("channel_len_minus_avg"),
    )
)

labeled = (
    vs_group_2
    .filter(F.col("event_at") >= F.lit("2025-02-06 12:00:00").cast("timestamp"))
    .select(
        "id",
        "channel_len",
        "channel_len_minus_median",
        "median_median_channel_len",
        "event_id",
        "event_actor",
        "event_type",
        "event_at",
        "avg_channel_len",
        "channel_len_minus_avg",
        F.concat_ws(": ", F.col("event_actor"), F.col("event_type")).alias("event_actor_event_type_label"),
        F.substring(F.trim(F.col("event_actor")), 1, 2).alias("event_actor_prefix"),
        F.concat(F.col("event_actor"), F.lit("#"), F.col("event_id").cast("string")).alias("event_actor_ref"),
    )
)

result = (
    labeled
    .groupBy("channel_len", "event_at")
    .pivot("event_type", ["replied", "reopened"])
    .agg(F.min("avg_channel_len"))
    .orderBy("channel_len", "event_at")
)
