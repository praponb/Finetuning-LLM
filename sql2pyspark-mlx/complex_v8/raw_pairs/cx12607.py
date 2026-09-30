from pyspark.sql import functions as F

base = (
    df_ticket_events
    .filter(F.col("ticket_id") <= 6)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.month(F.col("event_at")).alias("event_at_month"),
    )
)

detail_rows = (
    base.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type") == "escalated"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at_month",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor"),
    )
)

with_children_events = (
    detail_rows.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.hour(F.col("t.event_at")).between(9, 11)), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at_month",
        "s.event_id",
        "s.event_event_type",
        "s.event_actor",
        F.col("t.id").alias("event_id_2"),
        "t.event_at",
        F.col("t.actor").alias("event_actor_2"),
        "t.event_type",
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("actor").isin("agent", "customer"), 1)).alias("event_actor_count"),
        F.count(F.when(F.col("actor") == "system", 1)).alias("event_actor_count_2"),
    )
)

with_stats = (
    with_children_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
        F.coalesce(F.col("t.event_actor_count_2"), F.lit(0)).alias("event_actor_count_2"),
    )
)

result = (
    with_stats
    .groupingSets(
        [["event_count", "event_actor", "event_actor_2"], ["event_count"], ["event_actor"], []],
        "event_count", "event_actor", "event_actor_2",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("event_id_2").alias("distinct_event_id_2s"),
        F.countDistinct("event_id").alias("distinct_event_ids"),
    )
)
