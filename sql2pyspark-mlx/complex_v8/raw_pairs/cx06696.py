from pyspark.sql import functions as F

prepared_events = (
    df_ticket_events
    .filter(F.col("actor") == "customer")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        (F.col("event_at") + F.expr("INTERVAL 15 MINUTES")).alias("event_at_plus_15m"),
        F.to_date(F.col("event_at")).alias("event_at_date"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.count(F.when(F.col("actor") != "customer", 1)).alias("event_actor_count"),
        F.max("event_at").alias("last_event_at_2"),
    )
)

profiled = (
    prepared_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
        "t.last_event_at_2",
    )
)

totals = (
    profiled
    .groupBy(
        "actor",
        F.date_trunc("month", "event_at_date").alias("event_at_date_month"),
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("actor") == "system", 1)) / F.count("*"), 1).alias("pct_actor_system"),
        F.sum(F.when(F.col("event_type") == "replied", F.col("event_actor_count"))).alias("event_actor_count_event_type_replied"),
        F.sum(F.when(~F.col("event_type").isin("resolved", "created"), F.col("event_actor_count")).otherwise(0)).alias("event_actor_count_event_type_not_in_resolved_c"),
        F.round(100 * F.count(F.when(F.col("event_type") == "assigned", 1)) / F.count("*"), 1).alias("pct_event_type_assigned"),
        F.percentile_approx("event_actor_count", 0.5).alias("median_event_actor_count"),
        F.count("*").alias("row_count"),
    )
)

scored = (
    totals
    .filter(F.col("pct_actor_system") <= 75.0)
    .select(
        "*",
        F.lower(F.trim(F.col("actor"))).alias("actor_clean"),
        F.substring(F.trim(F.col("actor")), 1, 3).alias("actor_prefix"),
        F.upper(F.trim(F.col("actor"))).alias("actor_clean_2"),
    )
)

result = (
    scored
    .filter(F.col("pct_event_type_assigned") >= 20.0)
    .select(
        "actor",
        "pct_actor_system",
        "event_actor_count_event_type_replied",
        "pct_event_type_assigned",
        "row_count",
        "actor_clean",
        "actor_prefix",
        F.lower(F.trim(F.col("actor_clean_2"))).alias("actor_clean_2_clean"),
        F.to_date(F.col("event_at_date_month")).alias("event_at_date_month_date"),
    )
    .orderBy("pct_event_type_assigned", F.col("actor_clean").asc_nulls_last())
)
