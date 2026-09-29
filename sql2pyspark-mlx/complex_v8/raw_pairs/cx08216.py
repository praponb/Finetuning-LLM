from pyspark.sql import functions as F

cleaned_events = (
    df_ticket_events
    .filter((F.col("event_at") >= F.lit("2025-01-13 12:00:00").cast("timestamp")) & (F.col("actor").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
        F.concat_ws("|", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
        F.concat(F.col("actor"), F.lit("#"), F.col("id").cast("string")).alias("actor_ref"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("actor").isNotNull(), 1)).alias("event_actor_count"),
        F.max("event_at").alias("last_event_at"),
    )
)

summarized_rows = (
    cleaned_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
        "t.last_event_at",
    )
)

line_level = (
    summarized_rows.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_count",
        "s.event_actor_count",
        "s.last_event_at",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor"),
    )
)

with_details_ticket = (
    line_level.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .filter(F.hour(F.col("t.opened_at")).between(8, 10))
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_count",
        "s.event_actor_count",
        "s.last_event_at",
        "s.event_id",
        "s.event_event_type",
        "s.event_event_at",
        "s.event_actor",
        F.col("t.opened_at").alias("ticket_opened_at"),
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
    )
)

result = (
    with_details_ticket
    .cube("agent_team", "event_actor_count")
    .agg(
        F.count("*").alias("row_count"),
        F.round(100 * F.count(F.when(F.upper(F.trim(F.col("agent_name"))).like("J%"), 1)) / F.count("*"), 1).alias("pct_upper_trim_agent_name_like_j"),
    )
)
