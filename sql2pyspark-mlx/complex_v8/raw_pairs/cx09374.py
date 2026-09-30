from pyspark.sql import functions as F

recent = (
    df_ticket_events
    .filter(F.col("event_at") < F.lit("2025-02-18 10:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
        F.dayofweek(F.col("event_at")).alias("event_at_dayofweek"),
        F.substring(F.trim(F.col("event_type")), 1, 3).alias("event_type_prefix"),
    )
)

with_details = (
    recent.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_at_dayofweek",
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(F.col("actor") == "system", 1)).alias("event_actor_count"),
        F.max("event_at").alias("last_event_at"),
    )
)

with_stats = (
    with_details.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
        "t.last_event_at",
    )
)

filtered = (
    with_stats.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (~F.col("t.actor").isin("agent", "customer")), "left_anti")
    .filter(F.col("event_actor_count") < 5)
)

line_level = (
    filtered.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type") == "escalated"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.actor",
        "s.event_actor_count",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

result = (
    line_level
    .filter(~F.col("event_actor").isin("customer", "agent"))
    .select(
        "id",
        "event_actor_count",
        "event_actor",
        F.when(F.col("event_actor_count") >= 7, "HIGH").when(F.col("event_actor_count") >= 2, "MEDIUM").otherwise("LOW").alias("event_actor_count_tier"),
    )
    .orderBy("event_actor_count", F.col("event_actor").asc())
)
