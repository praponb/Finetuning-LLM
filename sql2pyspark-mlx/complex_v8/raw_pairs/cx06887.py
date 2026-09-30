from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("agent_id", "event_actor").orderBy(F.col("event_at").desc(), F.col("id").desc(), F.col("event_id").desc())

scoped_tickets = (
    df_tickets
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        (F.col("closed_at") + F.expr("INTERVAL 7 DAYS")).alias("closed_at_plus_7d"),
    )
)

labeled = (
    scoped_tickets
    .filter(F.col("closed_at") >= F.lit("2025-01-14 08:00:00").cast("timestamp"))
    .select(
        "*",
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
    )
)

exploded_detail = (
    labeled.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.channel",
        "s.csat",
        "s.priority_or_unknown",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_at",
        "t.event_type",
    )
)

latest = (
    exploded_detail
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest
    .select(
        "id",
        "agent_id",
        "event_id",
        "event_actor",
        "event_type",
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label"),
    )
    .orderBy(F.col("event_id").desc(), "id")
    .limit(10)
)
