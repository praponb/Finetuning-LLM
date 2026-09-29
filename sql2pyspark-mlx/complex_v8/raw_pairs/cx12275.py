from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("priority_or_unknown", "event_actor").orderBy("csat", "id", "event_id")

base_tickets = (
    df_tickets
    .filter((F.hour(F.col("opened_at")).between(9, 15)) & (F.array_contains(F.col("tags"), "bug")))
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
        "tags",
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
        F.least(F.col("csat"), F.col("requester_id")).alias("least_csat_requester_id"),
    )
)

labeled = (
    base_tickets
    .filter(F.col("agent_id") >= 8)
    .select(
        "id",
        "closed_at",
        "csat",
        "tags",
        "channel_or_unknown",
        "least_csat_requester_id",
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
    )
)

detail_rows_events = (
    labeled.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.actor") == "agent"), "inner")
    .select(
        "s.id",
        "s.closed_at",
        "s.csat",
        "s.channel_or_unknown",
        "s.priority_or_unknown",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        F.col("t.actor").alias("event_actor"),
    )
)

deduped = (
    detail_rows_events
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.count("*").alias("event_count"),
    )
)

with_stats = (
    deduped.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.first_event_at",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

with_details_event = (
    with_stats.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.id") == F.col("s.event_id"), "inner")
    .join(df_tickets.alias("ti"), F.col("ti.id") == F.col("t.ticket_id"), "inner")
    .filter(F.col("t.event_type").isNotNull())
    .select(
        "s.id",
        "s.closed_at",
        "s.csat",
        "s.channel_or_unknown",
        "s.event_id",
        "s.event_at",
        "s.event_actor",
        "t.event_type",
        F.col("ti.requester_id").alias("ticket_requester_id"),
    )
)

result = (
    with_details_event
    .select(
        "id",
        "closed_at",
        "csat",
        "channel_or_unknown",
        "event_at",
        "event_actor",
        "event_type",
        "ticket_requester_id",
    )
)
