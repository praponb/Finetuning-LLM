from pyspark.sql import functions as F

scoped = (
    df_ticket_events
    .filter(F.col("event_type") == "escalated")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "MMM yyyy").alias("event_at_label"),
        F.substring(F.trim(F.col("event_type")), 1, 4).alias("event_type_prefix"),
    )
)

labeled = (
    scoped
    .withColumn("days_since_event_at", F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")))
)

line_level = (
    labeled.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor") == "agent"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.event_type_prefix",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

result = (
    line_level
    .groupingSets(
        [["event_type", "ticket_id"], ["event_type"]],
        "event_type", "ticket_id",
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("event_actor").isNotNull(), 1)) / F.count("*"), 1).alias("pct_event_actor_is_not_null"),
    )
    .select(
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        "ticket_id",
        "pct_event_actor_is_not_null",
    )
    .orderBy(
        F.col("event_type").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
    )
)
