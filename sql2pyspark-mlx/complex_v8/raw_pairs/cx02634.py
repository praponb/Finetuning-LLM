from pyspark.sql import functions as F

filtered_events = (
    df_ticket_events
    .filter((F.col("actor").isNotNull()) | (F.month(F.col("event_at")).isin(1, 3)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        (F.col("event_at") + F.expr("INTERVAL 2 HOURS")).alias("event_at_plus_2h"),
        F.date_format(F.col("event_at"), "yyyy-MM-dd").alias("event_at_label"),
    )
)

totals = (
    filtered_events
    .groupBy(F.year("event_at_plus_2h").alias("event_at_plus_2h_year"), "ticket_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.round(100 * F.count(F.when(F.col("event_type") == "resolved", 1)) / F.count("*"), 1).alias("pct_event_type_resolved"),
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("event_type").isin("created", "resolved"), 1)).alias("n_event_type_in_created_resolv"),
        F.countDistinct("event_type").alias("distinct_event_types"),
    )
)

screened = (
    totals.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left_semi")
)

scored = (
    screened
    .filter(F.col("distinct_ids") >= 4)
    .select(
        "*",
        F.coalesce(F.col("pct_event_type_resolved"), F.lit(0.0)).alias("pct_event_type_resolved_filled"),
        F.when(F.col("distinct_event_types") >= 7, "large").when(F.col("distinct_event_types") >= 5, "medium").otherwise("small").alias("distinct_event_types_tier"),
        (F.col("event_at_plus_2h_year") - F.col("distinct_ids")).alias("event_at_plus_2h_year_minus_distinct_ids"),
    )
)

enriched_ticket = (
    scored.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.channel").isNotNull())
    .select(
        "s.*",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.min("event_at").alias("first_event_at"))
)

with_stats = (
    enriched_ticket.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.first_event_at")
)

result = (
    with_stats
    .select(
        "event_at_plus_2h_year",
        "ticket_id",
        "pct_event_type_resolved",
        "pct_event_type_resolved_filled",
    )
    .unpivot(
        ids=["event_at_plus_2h_year", "ticket_id"],
        values=["pct_event_type_resolved", "pct_event_type_resolved_filled"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("event_at_plus_2h_year", "ticket_id", "measure")
)
