from pyspark.sql import functions as F

prepared_events = (
    df_ticket_events
    .filter(F.col("actor") == "system")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
    )
)

line_level_events = (
    prepared_events.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor_slug",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor"),
    )
)

with_children = (
    line_level_events.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type").isNotNull()), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.event_id",
        "s.event_event_type",
        F.col("t.id").alias("event_id_2"),
        "t.event_at",
        F.col("t.actor").alias("event_actor_2"),
        F.col("t.event_type").alias("event_event_type_2"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.count(F.when(F.col("actor").isNotNull(), 1)).alias("event_actor_count"),
        F.min("event_at").alias("first_event_at_2"),
    )
)

with_stats = (
    with_children.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.first_event_at",
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
        "t.first_event_at_2",
    )
)

result = (
    with_stats
    .filter(F.col("event_id").isNotNull())
    .select("event_event_type_2", "id", "event_at")
    .distinct()
    .orderBy(F.col("event_event_type_2").desc(), F.col("id").asc())
)
