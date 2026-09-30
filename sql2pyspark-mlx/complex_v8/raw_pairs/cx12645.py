from pyspark.sql import functions as F

valid = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.substring(F.trim(F.col("event_type")), 1, 4).alias("event_type_prefix"),
    )
)

screened = (
    valid.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type") == "resolved"), "left_anti")
)

filtered = (
    screened.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left_semi")
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(F.col("actor") != "customer", 1)).alias("event_actor_count"),
    )
)

with_counts = (
    filtered.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
    )
)

rollup_base = (
    with_counts
    .filter(F.col("event_type_prefix").isNotNull())
    .groupBy("event_actor_count", "event_type", "actor")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.count("*").alias("n_rows"),
        F.max("days_since_event_at").alias("max_days_since_event_at_check"),
    )
    .filter(F.col("max_days_since_event_at_check") > 70)
    .drop("max_days_since_event_at_check")
)

filtered_2 = (
    rollup_base
    .crossJoin(rollup_base.agg(F.avg("n_rows").alias("overall_avg_n_rows")))
    .filter(F.col("n_rows") > F.col("overall_avg_n_rows"))
    .drop("overall_avg_n_rows")
)

result = (
    filtered_2
    .filter(F.col("event_type") == "resolved")
    .select(
        "event_actor_count",
        "event_type",
        "actor",
        "distinct_event_types",
        "n_rows",
    )
)
