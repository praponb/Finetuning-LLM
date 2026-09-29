from pyspark.sql import functions as F

base = (
    df_ticket_events
    .filter(F.col("actor") != "customer")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        (F.col("event_at") + F.expr("INTERVAL 30 MINUTES")).alias("event_at_plus_30m"),
    )
)

detail_rows = (
    base.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

labeled = (
    detail_rows
    .filter(~F.col("event_type").isin("assigned", "resolved"))
    .select(
        "*",
        F.length(F.trim(F.col("event_event_type"))).alias("event_event_type_len"),
    )
)

result = (
    labeled
    .filter(F.col("ticket_id") >= 40)
    .select("event_type", "event_id")
    .distinct()
    .orderBy(F.col("event_id").desc())
)
