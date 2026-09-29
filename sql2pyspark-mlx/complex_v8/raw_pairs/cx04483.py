from pyspark.sql import functions as F

scoped = (
    df_ticket_events
    .filter(F.col("event_type") == "resolved")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.dayofweek(F.col("event_at")).alias("event_at_dayofweek"),
    )
)

joined_ticket = (
    scoped.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.requester_id") > 11), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.actor",
        "s.days_since_event_at",
        "s.event_at_dayofweek",
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

scored = (
    joined_ticket
    .select(
        "id",
        "ticket_id",
        "actor",
        "days_since_event_at",
        "event_at_dayofweek",
        "ticket_requester_id",
        F.round(F.col("days_since_event_at") / F.nullif(F.col("event_at_dayofweek"), F.lit(0)), 3).alias("days_since_event_at_per_event_at_dayofweek"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        F.substring(F.trim(F.col("actor")), 1, 3).alias("actor_prefix"),
    )
)

result = (
    scored
    .rollup("actor", "ticket_id")
    .agg(
        F.percentile_approx("days_since_event_at_per_event_at_dayofweek", 0.5).alias("median_days_since_event_at_per_event_at_dayofwee"),
        F.round(F.sum("days_since_event_at_per_event_at_dayofweek"), 2).alias("total_days_since_event_at_per_event_at_dayofweek"),
    )
    .select(
        F.coalesce(F.col("actor"), F.lit("ALL")).alias("actor"),
        "ticket_id",
        "median_days_since_event_at_per_event_at_dayofwee",
        "total_days_since_event_at_per_event_at_dayofweek",
    )
    .orderBy(F.col("actor").asc_nulls_last(), F.col("ticket_id").asc_nulls_last())
)
