from pyspark.sql import functions as F

valid_events = (
    df_ticket_events
    .filter((F.col("event_at") < F.lit("2025-02-24 08:00:00").cast("timestamp")) & (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.min("event_at").alias("first_event_at_2"),
        F.max("event_at").alias("last_event_at"),
    )
)

with_stats = (
    valid_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.first_event_at",
        "t.first_event_at_2",
        "t.last_event_at",
    )
)

tagged = (
    with_stats
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-03-08").cast("date"))
    .select(
        "*",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("first_event_at")).alias("days_since_first_event_at"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.when(F.col("actor").isin("agent", "customer"), "group_a").when(F.col("actor") == "system", "group_b").otherwise("other").alias("actor_group"),
    )
)

event_stats_2 = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at_2"),
        F.count(F.when(~F.col("event_type").isin("reopened", "assigned"), 1)).alias("event_event_type_count"),
    )
)

with_counts = (
    tagged.alias("s")
    .join(event_stats_2.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.last_event_at_2",
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
    )
)

result = (
    with_counts
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "first_event_at",
        "last_event_at",
        "days_since_first_event_at",
        "event_type_or_unknown",
        "actor_group",
        "event_event_type_count",
        F.dayofweek(F.col("event_at")).alias("event_at_dayofweek"),
    )
    .orderBy(F.col("event_at").desc(), F.col("actor").asc_nulls_last())
)
