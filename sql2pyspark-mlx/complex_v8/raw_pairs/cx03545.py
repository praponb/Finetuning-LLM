from pyspark.sql import functions as F

filtered = (
    df_ticket_events
    .filter((F.col("ticket_id") >= 45) | (F.col("event_at") < F.lit("2025-02-24 10:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.quarter(F.col("event_at")).alias("event_at_quarter"),
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
        (F.col("event_at") + F.expr("INTERVAL 15 MINUTES")).alias("event_at_plus_15m"),
    )
)

summary = (
    filtered
    .filter(F.col("actor_or_unknown") != "customer")
    .groupBy("event_type")
    .agg(
        F.sum("event_at_quarter").alias("total_event_at_quarter"),
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
        F.countDistinct("ticket_id").alias("distinct_ticket_ids"),
    )
    .filter(F.col("total_event_at_quarter") > 2)
)

qualified = (
    summary
    .crossJoin(summary.agg(F.min("distinct_ticket_ids").alias("overall_min_distinct_ticket_ids")))
    .filter(F.col("distinct_ticket_ids") > F.col("overall_min_distinct_ticket_ids"))
    .drop("overall_min_distinct_ticket_ids")
)

qualified_2 = (
    qualified
    .crossJoin(qualified.agg(F.avg("total_event_at_quarter").alias("overall_avg_total_event_at_quarter")))
    .filter(F.col("total_event_at_quarter") > F.col("overall_avg_total_event_at_quarter"))
    .drop("overall_avg_total_event_at_quarter")
)

result = (
    qualified_2
    .select("event_type", "total_event_at_quarter", "actor_list")
    .orderBy("total_event_at_quarter")
)
