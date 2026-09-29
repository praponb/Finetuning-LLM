from pyspark.sql import functions as F

recent_events = (
    df_ticket_events
    .filter((F.col("event_type") != "assigned") & (F.to_date(F.col("event_at")) < F.lit("2025-02-24").cast("date")))
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(F.col("event_type") == "assigned", 1)).alias("event_event_type_count"),
        F.count("*").alias("event_count"),
        F.max("event_at").alias("last_event_at"),
    )
)

summarized_rows = (
    recent_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        "t.last_event_at",
    )
)

labeled = (
    summarized_rows
    .filter(F.col("last_event_at") >= F.lit("2025-01-17 09:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_event_type_count",
        F.abs(F.col("event_count") - 3).alias("event_count_distance"),
    )
)

exploded_detail_events = (
    labeled.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_count_distance",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

result = (
    exploded_detail_events
    .select(
        "id",
        "event_at",
        "event_count_distance",
        "event_id",
        "event_actor",
        "event_event_type",
        F.abs(F.col("event_count_distance") - 4).alias("event_count_distance_distance"),
        F.date_trunc("day", F.col("event_at")).alias("event_at_day"),
    )
    .orderBy(F.col("event_id").desc())
)
