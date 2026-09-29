from pyspark.sql import functions as F

recent_events = (
    df_ticket_events
    .filter(F.col("actor") == "system")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
        F.when(F.col("event_type").isin("reopened", "escalated"), "group_a").when(F.col("event_type") == "replied", "group_b").otherwise("other").alias("event_type_group"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("event_type").isin("escalated", "assigned"), 1)).alias("event_event_type_count"),
    )
)

summarized_rows = (
    recent_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
    )
)

combined_details = (
    summarized_rows.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_type_slug",
        "s.event_type_group",
        "s.last_event_at",
        "s.event_event_type_count",
        F.col("t.agent_id").alias("ticket_agent_id"),
    )
)

totals = (
    combined_details
    .groupBy(F.date_format("last_event_at", "yyyy-MM").alias("last_event_at_ym"))
    .agg(
        F.round(F.avg("event_event_type_count"), 2).alias("avg_event_event_type_count"),
        F.countDistinct("actor").alias("distinct_actors"),
        F.max("event_at").alias("last_event_at_2"),
        F.min("last_event_at").alias("first_last_event_at"),
    )
)

filtered = (
    totals
    .crossJoin(totals.agg(F.max("avg_event_event_type_count").alias("overall_max_avg_event_event_type_count")))
    .filter(F.col("avg_event_event_type_count") < F.col("overall_max_avg_event_event_type_count"))
    .drop("overall_max_avg_event_event_type_count")
)

result = (
    filtered
    .filter(F.col("distinct_actors") < 6)
    .select(
        "last_event_at_ym",
        "avg_event_event_type_count",
        "distinct_actors",
        "last_event_at_2",
        F.length(F.trim(F.col("last_event_at_ym"))).alias("last_event_at_ym_len"),
    )
    .orderBy(
        F.col("avg_event_event_type_count").asc_nulls_last(),
        F.col("last_event_at_ym").asc(),
    )
)
