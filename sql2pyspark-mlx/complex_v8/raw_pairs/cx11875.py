from pyspark.sql import functions as F

base = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_activity = (
    base.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_at") >= F.lit("2025-01-25 10:00:00").cast("timestamp")) & (~F.col("t.event_type").isin("replied", "created")), "left_semi")
)

tagged = (
    with_activity
    .withColumn("event_type_ref", F.concat(F.col("event_type"), F.lit("#"), F.col("id").cast("string")))
)

with_details = (
    tagged.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_type_ref",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.tags").alias("ticket_tags"),
    )
)

result = (
    with_details
    .groupBy("ticket_agent_id", "event_type")
    .pivot("actor", ["customer", "system"])
    .agg(F.max("ticket_csat"))
)
