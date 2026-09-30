from pyspark.sql import functions as F

valid = (
    df_ticket_events
    .filter(F.col("event_type").isin("replied", "reopened"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label"),
    )
)

qualified = (
    valid.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left_semi")
)

tagged = (
    qualified
    .select(
        "*",
        F.length(F.trim(F.col("event_at_label"))).alias("event_at_label_len"),
    )
)

summary = (
    tagged
    .groupBy("ticket_id")
    .agg(
        F.countDistinct("ticket_id").alias("distinct_ticket_ids"),
        F.sum("event_at_label_len").alias("total_event_at_label_len"),
    )
    .filter(F.col("total_event_at_label_len") > 6)
)

detail_rows = (
    summary.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.ticket_id",
        "s.total_event_at_label_len",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        F.col("t.actor").alias("event_actor"),
    )
)

result = (
    detail_rows
    .cube("event_id", "ticket_id")
    .agg(
        F.sum("total_event_at_label_len").alias("total_total_event_at_label_len"),
        F.max("event_at").alias("last_event_at"),
    )
)
