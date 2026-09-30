from pyspark.sql import functions as F

scoped_events = (
    df_ticket_events
    .filter(~F.col("event_type").isin("reopened", "resolved"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("actor")), 1, 4).alias("actor_prefix"),
        F.date_format(F.col("event_at"), "MMM yyyy").alias("event_at_label"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
    )
)

enriched = (
    scoped_events
    .filter(F.col("actor_prefix").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "actor_prefix",
        "event_at_label",
        "event_type_slug",
        F.to_date(F.col("event_at")).alias("event_at_date"),
    )
)

result = (
    enriched
    .rollup("ticket_id", "event_type")
    .agg(
        F.grouping("ticket_id").alias("is_total_ticket_id"),
        F.min("event_at").alias("first_event_at"),
        F.round(100 * F.count(F.when(F.col("event_type") == "assigned", 1)) / F.count("*"), 1).alias("pct_event_type_assigned"),
    )
    .orderBy(
        F.col("ticket_id").asc_nulls_last(),
        F.col("event_type").asc_nulls_last(),
    )
)
