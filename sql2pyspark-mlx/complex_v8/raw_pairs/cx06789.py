from pyspark.sql import functions as F

scoped_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.date_format(F.col("event_at"), "yyyy-MM-dd").alias("event_at_label"),
    )
)

with_details = (
    scoped_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.priority") == "P1")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.days_since_event_at",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.priority").alias("ticket_priority"),
    )
)

scored = (
    with_details
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "ticket_csat",
        "ticket_priority",
        F.concat(F.col("ticket_priority"), F.lit("#"), F.col("ticket_id").cast("string")).alias("ticket_priority_ref"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("actor").isin("system", "customer"), 1)).alias("event_actor_count"),
    )
)

with_stats = (
    scored.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.first_event_at",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
    )
)

ticket_id_stats = (
    with_stats
    .groupBy("ticket_id")
    .agg(F.percentile_approx("event_actor_count", 0.5).alias("median_event_actor_count"))
)

above_average = (
    with_stats.alias("s")
    .join(ticket_id_stats.alias("g"), F.col("g.ticket_id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("s.event_actor_count") >= F.col("g.median_event_actor_count"))
    .select(
        "s.*",
        "g.median_event_actor_count",
        F.round(F.col("s.event_actor_count") - F.col("g.median_event_actor_count"), 2).alias("event_actor_count_minus_median"),
    )
)

labeled = (
    above_average
    .withColumn("ticket_priority_or_unknown", F.coalesce(F.col("ticket_priority"), F.lit("UNKNOWN")))
)

with_details_ticket = (
    labeled.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.csat") <= 4), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.ticket_csat",
        "s.ticket_priority",
        "s.ticket_priority_ref",
        "s.first_event_at",
        "s.median_event_actor_count",
        "s.event_actor_count_minus_median",
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.csat").alias("ticket_csat_2"),
    )
)

result = (
    with_details_ticket
    .select("id", "ticket_csat", "ticket_csat_2")
    .unpivot(
        ids=["id"],
        values=["ticket_csat", "ticket_csat_2"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
