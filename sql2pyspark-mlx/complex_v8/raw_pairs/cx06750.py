from pyspark.sql import functions as F

prepared_events = (
    df_ticket_events
    .filter(F.col("event_at") < F.lit("2025-02-12 08:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.upper(F.trim(F.col("actor"))).alias("actor_clean"),
    )
)

joined = (
    prepared_events.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.opened_at") < F.lit("2025-03-02 08:00:00").cast("timestamp")), "left")
    .select(
        "s.*",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

result = (
    joined
    .cube("event_type", "ticket_channel")
    .agg(
        F.grouping("event_type").alias("is_total_event_type"),
        F.countDistinct("ticket_channel").alias("distinct_ticket_channels"),
    )
    .select(
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        F.coalesce(F.col("ticket_channel"), F.lit("ALL")).alias("ticket_channel"),
        "is_total_event_type",
        "distinct_ticket_channels",
    )
    .orderBy(
        F.col("event_type").asc_nulls_last(),
        F.col("ticket_channel").asc_nulls_last(),
    )
)
