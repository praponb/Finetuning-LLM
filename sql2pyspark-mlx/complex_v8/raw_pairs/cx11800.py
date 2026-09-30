from pyspark.sql import functions as F

recent = (
    df_ticket_events
    .filter(F.col("actor").isin("customer", "agent"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
    )
)

without_matches = (
    recent.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.hour(F.col("t.event_at")).between(10, 15)), "left_anti")
    .filter(~F.col("event_type").isin("escalated", "created"))
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.max("event_at").alias("last_event_at"),
    )
)

profiled = (
    without_matches.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.first_event_at", "t.last_event_at")
)

result = (
    profiled
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "actor_slug",
        "first_event_at",
        "last_event_at",
        F.substring(F.trim(F.col("actor_slug")), 1, 3).alias("actor_slug_prefix"),
    )
    .orderBy(F.col("ticket_id").desc(), "event_at", "id")
    .limit(10)
)
