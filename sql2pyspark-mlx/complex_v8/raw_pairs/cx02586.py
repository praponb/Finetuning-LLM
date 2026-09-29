from pyspark.sql import functions as F

scoped = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.hour(F.col("event_at")).alias("event_at_hour"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at_2"),
    )
)

ticket_id_stats = (
    scoped
    .groupBy("ticket_id")
    .agg(F.percentile_approx("days_since_event_at", 0.5).alias("median_days_since_event_at"))
)

vs_group = (
    scoped.alias("s")
    .join(ticket_id_stats.alias("g"), F.col("g.ticket_id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("s.days_since_event_at") >= F.col("g.median_days_since_event_at"))
    .select(
        "s.*",
        "g.median_days_since_event_at",
        F.round(F.col("s.days_since_event_at") - F.col("g.median_days_since_event_at"), 2).alias("days_since_event_at_minus_median"),
    )
)

scored = (
    vs_group
    .select(
        "*",
        F.date_trunc("hour", F.col("event_at")).alias("event_at_hour_2"),
        F.coalesce(F.col("event_at_hour"), F.lit(0)).alias("event_at_hour_filled"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
    )
)

result = (
    scored
    .rollup("actor", "event_at_hour", "ticket_id")
    .agg(
        F.count(F.when(F.col("event_type") == "reopened", 1)).alias("n_event_type_reopened"),
        F.round(100 * F.count(F.when(F.col("actor").isin("customer", "system"), 1)) / F.count("*"), 1).alias("pct_actor_in_customer_system"),
        F.round(F.sum("days_since_event_at") / F.nullif(F.sum("days_since_event_at_2"), F.lit(0)), 3).alias("days_since_event_at_to_days_since_event_at_2_rat"),
    )
    .orderBy(
        F.col("actor").asc_nulls_last(),
        F.col("event_at_hour").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
    )
)
