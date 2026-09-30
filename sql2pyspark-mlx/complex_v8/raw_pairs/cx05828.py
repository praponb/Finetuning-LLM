from pyspark.sql import functions as F

base = (
    df_ticket_events
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-02-12").cast("date"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

scored = (
    base
    .select(
        "*",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.concat_ws(" / ", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
    )
)

result = (
    scored
    .groupingSets(
        [["actor", "event_type", "ticket_id"], ["actor"], ["event_type"]],
        "actor", "event_type", "ticket_id",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.sum("days_since_event_at").alias("total_days_since_event_at"),
        F.percentile_approx("days_since_event_at", 0.5).alias("median_days_since_event_at"),
    )
    .select(
        F.coalesce(F.col("actor"), F.lit("ALL")).alias("actor"),
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        "ticket_id",
        "grouping_level",
        "total_days_since_event_at",
        "median_days_since_event_at",
    )
    .orderBy(
        F.col("actor").asc_nulls_last(),
        F.col("event_type").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
    )
)
