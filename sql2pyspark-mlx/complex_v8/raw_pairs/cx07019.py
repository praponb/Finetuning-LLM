from pyspark.sql import functions as F

scoped_events = (
    df_ticket_events
    .filter((F.col("event_at") < F.lit("2025-03-02 09:00:00").cast("timestamp")) & (F.col("event_type").isin("replied", "escalated")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("event_type").isin("created", "reopened"), "group_a").when(F.col("event_type") == "resolved", "group_b").otherwise("other").alias("event_type_group"),
    )
)

enriched = (
    scoped_events
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("event_type_group")), " ", "_")).alias("event_type_group_slug"),
        F.upper(F.trim(F.col("event_type_group"))).alias("event_type_group_clean"),
    )
)

with_parent = (
    enriched.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_type_group",
        "s.event_type_group_slug",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

result = (
    with_parent
    .rollup("ticket_channel", "event_type", "actor")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
    )
)
