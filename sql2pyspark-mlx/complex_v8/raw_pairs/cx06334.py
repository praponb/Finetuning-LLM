from pyspark.sql import functions as F

base_events = (
    df_ticket_events
    .filter(F.col("event_type") == "replied")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.lower(F.trim(F.col("actor"))).alias("actor_clean"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
    )
)

stats = (
    base_events
    .groupBy(
        "event_type",
        F.date_format("event_at", "yyyy-MM").alias("event_at_ym"),
        "ticket_id",
    )
    .agg(
        F.count(F.when(F.col("actor").isin("system", "agent"), 1)).alias("n_actor_in_system_agent"),
        F.count("*").alias("n_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("actor").alias("distinct_actors"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.count("*").alias("event_count"))
)

with_counts = (
    stats.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

with_children = (
    with_counts.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.event_type",
        "s.event_at_ym",
        "s.ticket_id",
        "s.n_actor_in_system_agent",
        "s.n_rows",
        "s.distinct_ids",
        "s.distinct_actors",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.actor").alias("event_actor"),
    )
)

scored = (
    with_children
    .select(
        "event_type",
        "event_at_ym",
        "ticket_id",
        "n_actor_in_system_agent",
        "n_rows",
        "event_id",
        "event_actor",
        F.when(F.col("event_actor").isin("agent", "system"), "group_a").when(F.col("event_actor") == "customer", "group_b").otherwise("other").alias("event_actor_group"),
        F.length(F.trim(F.col("event_event_type"))).alias("event_event_type_len"),
    )
)

result = (
    scored
    .select(
        "event_type",
        "event_at_ym",
        "n_actor_in_system_agent",
        "n_rows",
        "event_id",
        "event_actor_group",
        "event_event_type_len",
    )
    .orderBy(F.col("n_actor_in_system_agent").desc(), F.col("event_id").desc())
)
