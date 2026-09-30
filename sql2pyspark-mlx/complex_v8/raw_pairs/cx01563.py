from pyspark.sql import functions as F

scoped_events = (
    df_ticket_events
    .filter((F.col("event_at") >= F.lit("2025-01-31 13:00:00").cast("timestamp")) | (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label"),
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label_2"),
    )
)

per_group = (
    scoped_events
    .groupBy(F.date_trunc("month", "event_at").alias("event_at_month"))
    .agg(
        F.count(F.when(F.col("actor").isin("system", "customer"), 1)).alias("n_actor_in_system_customer"),
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
        F.round(100 * F.count(F.when(F.col("event_type") != "replied", 1)) / F.count("*"), 1).alias("pct_event_type_replied"),
    )
)

screened = (
    per_group
    .crossJoin(per_group.agg(F.min("pct_event_type_replied").alias("overall_min_pct_event_type_replied")))
    .filter(F.col("pct_event_type_replied") > F.col("overall_min_pct_event_type_replied"))
    .drop("overall_min_pct_event_type_replied")
)

derived = (
    screened
    .filter(F.col("n_actor_in_system_customer") >= 2)
    .select(
        "event_at_month",
        "pct_event_type_replied",
        F.date_format(F.col("event_at_month"), "MMM yyyy").alias("event_at_month_label"),
        F.coalesce(F.col("pct_event_type_replied"), F.lit(0.0)).alias("pct_event_type_replied_filled"),
    )
)

with_activity = (
    derived
    .crossJoin(derived.agg(F.avg("pct_event_type_replied_filled").alias("overall_avg_pct_event_type_replied_filled")))
    .filter(F.col("pct_event_type_replied_filled") > F.col("overall_avg_pct_event_type_replied_filled"))
    .drop("overall_avg_pct_event_type_replied_filled")
)

derived_2 = (
    with_activity
    .filter(F.hour(F.col("event_at_month")).between(7, 10))
    .select(
        "*",
        F.when(F.col("pct_event_type_replied") >= 60.0, "HIGH").when(F.col("pct_event_type_replied") >= 40.0, "MEDIUM").otherwise("LOW").alias("pct_event_type_replied_tier"),
        F.date_format(F.col("event_at_month"), "yyyy-MM-dd").alias("event_at_month_label_2"),
        F.date_trunc("day", F.col("event_at_month")).alias("event_at_month_day"),
    )
)

result = (
    derived_2
    .filter(F.col("pct_event_type_replied_filled") > 35.0)
    .select(
        "event_at_month",
        "pct_event_type_replied",
        "event_at_month_label",
        "pct_event_type_replied_filled",
        "pct_event_type_replied_tier",
        "event_at_month_label_2",
        "event_at_month_day",
        F.abs(F.col("pct_event_type_replied") - 65.0).alias("pct_event_type_replied_distance"),
        F.abs(F.col("pct_event_type_replied") - 45.0).alias("pct_event_type_replied_distance_2"),
    )
    .orderBy(
        "pct_event_type_replied_distance_2",
        F.col("event_at_month_label_2").asc(),
    )
)
