from pyspark.sql import functions as F

filtered = (
    df_ticket_events
    .filter(F.col("event_at") < F.lit("2025-02-24 13:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.to_date(F.col("event_at")).alias("event_at_date"),
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.concat(F.col("actor"), F.lit("#"), F.col("actor_len").cast("string")).alias("actor_ref"),
        F.date_trunc("year", F.col("event_at")).alias("event_at_year"),
    )
)

with_children_events = (
    scored.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type").isNotNull()), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor_len",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.count(F.when(F.col("actor") == "agent", 1)).alias("event_actor_count"),
    )
)

profiled = (
    with_children_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
    )
)

result = (
    profiled
    .groupBy("actor_len")
    .pivot("event_actor", ["system", "customer", "agent"])
    .agg(F.count("event_actor_count"))
    .orderBy("actor_len")
)
