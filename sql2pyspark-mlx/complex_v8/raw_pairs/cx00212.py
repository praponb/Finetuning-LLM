from pyspark.sql import functions as F

cleaned_events = (
    df_ticket_events
    .filter(F.month(F.col("event_at")).isin(1, 2))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

totals = (
    cleaned_events
    .filter(F.col("ticket_id").isNotNull())
    .groupBy("ticket_id", "event_type", "actor")
    .agg(
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.count("*").alias("n_rows"),
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.count(F.when(~F.col("event_type").isin("assigned", "reopened"), 1)).alias("n_event_type_not_in_assigned_r"),
    )
)

without_matches = (
    totals.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_at") < F.lit("2025-03-02 09:00:00").cast("timestamp")) & (~F.col("t.event_type").isin("assigned", "created")), "left_anti")
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.max("event_at").alias("last_event_at"))
)

with_counts = (
    without_matches.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.last_event_at")
)

annotated = (
    with_counts
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "ticket_id",
        "event_type",
        "actor",
        "event_type_list",
        "n_rows",
        "n_event_type_not_in_assigned_r",
        "last_event_at",
        F.substring(F.trim(F.col("actor")), 1, 2).alias("actor_prefix"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_type_ref"),
        F.coalesce(F.col("distinct_event_types"), F.lit(0)).alias("distinct_event_types_filled"),
    )
)

enriched_ticket = (
    annotated.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.event_type_list",
        "s.n_rows",
        "s.last_event_at",
        "s.distinct_event_types_filled",
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

result = (
    enriched_ticket
    .groupingSets(
        [["event_type", "n_rows", "ticket_id"], ["event_type"], []],
        "event_type", "n_rows", "ticket_id",
    )
    .agg(
        F.min("ticket_closed_at").alias("first_ticket_closed_at"),
        F.array_sort(F.collect_set("event_type")).alias("event_type_list_2"),
    )
    .select(
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        "n_rows",
        "ticket_id",
        "first_ticket_closed_at",
        "event_type_list_2",
    )
    .orderBy(
        F.col("event_type").asc_nulls_last(),
        F.col("n_rows").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
    )
)
