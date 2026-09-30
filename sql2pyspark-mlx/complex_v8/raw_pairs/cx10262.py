from pyspark.sql import functions as F

cleaned_events = (
    df_ticket_events
    .filter((F.col("event_at") >= F.lit("2025-01-25 13:00:00").cast("timestamp")) | (~F.col("actor").isin("agent", "system")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("actor")), 1, 2).alias("actor_prefix"),
        F.date_trunc("month", F.col("event_at")).alias("event_at_month"),
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.max("event_at").alias("last_event_at_2"),
        F.count("*").alias("event_count"),
    )
)

with_stats = (
    cleaned_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        "t.last_event_at_2",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

result = (
    with_stats
    .cube("ticket_id", "actor_or_unknown")
    .agg(
        F.count(F.when(F.col("event_type").isNotNull(), 1)).alias("n_event_type_is_not_null"),
    )
    .orderBy(
        F.col("ticket_id").asc_nulls_last(),
        F.col("actor_or_unknown").asc_nulls_last(),
    )
)
