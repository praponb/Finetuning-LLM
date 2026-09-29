from pyspark.sql import functions as F

filtered = (
    df_ticket_events
    .filter((F.col("event_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp")) & (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_type_ref"),
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
        F.length(F.trim(F.col("actor"))).alias("actor_len_2"),
    )
)

joined = (
    filtered.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_type_ref",
        "s.actor_len",
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(F.col("actor").isNotNull(), 1)).alias("event_actor_count"),
        F.count("*").alias("event_count"),
        F.max("event_at").alias("last_event_at"),
    )
)

summarized_rows = (
    joined.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        "t.last_event_at",
    )
)

result = (
    summarized_rows
    .rollup("event_type", "actor_len", "actor")
    .agg(
        F.round(F.sum("event_actor_count") / F.nullif(F.sum("actor_len"), F.lit(0)), 3).alias("event_actor_count_to_actor_len_ratio"),
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
        F.min("event_at").alias("first_event_at"),
    )
)
