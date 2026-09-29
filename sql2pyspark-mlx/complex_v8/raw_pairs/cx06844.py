from pyspark.sql import functions as F

base = (
    df_ticket_events
    .filter((F.col("actor") != "agent") & (F.hour(F.col("event_at")).between(10, 12)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("event_type").isin("escalated", "replied"), "group_a").when(F.col("event_type") == "reopened", "group_b").otherwise("other").alias("event_type_group"),
        F.month(F.col("event_at")).alias("event_at_month"),
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

with_children_events = (
    base.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor") == "system"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type_group",
        "s.event_at_month",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

result = (
    with_children_events
    .select(
        "id",
        "ticket_id",
        "event_type_group",
        "event_at_month",
        "event_id",
        "event_actor",
        "event_event_type",
        F.coalesce(F.col("event_at_month"), F.lit(0)).alias("event_at_month_filled"),
        F.length(F.trim(F.col("event_event_type"))).alias("event_event_type_len"),
    )
    .orderBy(F.col("event_event_type").desc_nulls_first())
)
