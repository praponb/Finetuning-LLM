from pyspark.sql import functions as F

filtered = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.col("actor") == "system"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws(": ", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

screened = (
    filtered.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type") != "assigned"), "left_semi")
)

result = (
    screened
    .cube("actor", "event_type")
    .agg(
        F.grouping("actor").alias("is_total_actor"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("days_since_event_at"), 2).alias("avg_days_since_event_at"),
    )
)
