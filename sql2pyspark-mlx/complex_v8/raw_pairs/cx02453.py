from pyspark.sql import functions as F

cleaned = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "csat",
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
    )
)

combined_details = (
    cleaned.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.csat",
        "s.priority_len",
        F.col("su.tier").alias("agent_tier"),
    )
)

derived = (
    combined_details
    .select(
        "*",
        F.concat(F.col("agent_tier"), F.lit("#"), F.col("id").cast("string")).alias("agent_tier_ref"),
    )
)

with_children = (
    derived.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.csat",
        "s.agent_tier",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        "t.event_at",
        F.col("t.actor").alias("event_actor"),
    )
)

event_actor_stats = (
    with_children
    .groupBy("event_actor")
    .agg(F.percentile_approx("csat", 0.5).alias("median_csat"))
)

vs_group = (
    with_children.alias("s")
    .join(event_actor_stats.alias("g"), F.col("g.event_actor") == F.col("s.event_actor"), "inner")
    .filter(F.col("s.csat") >= F.col("g.median_csat"))
    .select(
        "s.*",
        "g.median_csat",
        F.round(F.col("s.csat") - F.col("g.median_csat"), 2).alias("csat_minus_median"),
    )
)

result = (
    vs_group
    .select(
        "id",
        "agent_id",
        "csat",
        "agent_tier",
        "event_id",
        "event_type",
        "event_at",
        "event_actor",
        "median_csat",
        "csat_minus_median",
        (F.col("event_at") + F.expr("INTERVAL 1 DAYS")).alias("event_at_plus_1d"),
        (F.col("median_csat") + F.col("csat_minus_median")).alias("median_csat_plus_csat_minus_median"),
    )
    .orderBy("event_id", F.col("event_actor").desc())
)
