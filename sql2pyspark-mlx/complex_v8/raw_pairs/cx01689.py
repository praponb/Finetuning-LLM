from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("agent_id").orderBy("closed_at", "id")
w_dedup_2 = Window.partitionBy("agent_id", "channel").orderBy("event_at", "id", "event_id")

prepared = (
    df_tickets
    .filter(F.col("csat") <= 4)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

unique_rows = (
    prepared
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count("*").alias("ticket_count"),
        F.min("closed_at").alias("first_ticket_closed_at"),
        F.min("opened_at").alias("first_ticket_opened_at"),
    )
)

with_counts = (
    unique_rows.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        "t.first_ticket_closed_at",
        "t.first_ticket_opened_at",
    )
)

detail_rows_events = (
    with_counts.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.actor").isNotNull()), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.channel",
        "s.csat",
        "s.first_ticket_opened_at",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        F.col("t.actor").alias("event_actor"),
    )
)

latest_per_key = (
    detail_rows_events
    .withColumn("recency_rank", F.row_number().over(w_dedup_2))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

detail_rows = (
    latest_per_key.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.hour(F.col("t.event_at")).between(7, 11)), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.channel",
        "s.csat",
        "s.first_ticket_opened_at",
        "s.event_id",
        "s.event_at",
        "s.event_actor",
        F.col("t.id").alias("event_id_2"),
        F.col("t.actor").alias("event_actor_2"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

result = (
    detail_rows
    .select(
        "id",
        "closed_at",
        "channel",
        "csat",
        "first_ticket_opened_at",
        "event_id",
        "event_at",
        "event_actor",
        "event_id_2",
        "event_event_at",
        (F.col("event_at") + F.expr("INTERVAL 30 MINUTES")).alias("event_at_plus_30m"),
    )
    .orderBy(F.col("event_id").desc(), "id", "event_id_2")
    .limit(5)
)
