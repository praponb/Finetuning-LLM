from pyspark.sql import functions as F

base = (
    df_ticket_events
    .filter((F.month(F.col("event_at")).isin(2, 3)) & (F.col("ticket_id") >= 10))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.dayofweek(F.col("event_at")).alias("event_at_dayofweek"),
        F.substring(F.trim(F.col("actor")), 1, 2).alias("actor_prefix"),
    )
)

with_parent = (
    base.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.priority").alias("ticket_priority"),
    )
)

derived = (
    with_parent
    .filter(F.col("ticket_requester_id") >= 8)
    .select(
        "*",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.date_format(F.col("event_at"), "yyyy-MM-dd").alias("event_at_label"),
        (F.col("event_at") + F.expr("INTERVAL 15 MINUTES")).alias("event_at_plus_15m"),
    )
)

result = (
    derived
    .groupBy("event_type", "event_at_plus_15m")
    .pivot("actor", ["customer", "system", "agent"])
    .agg(F.sum("event_at_dayofweek").alias("amount"), F.max("event_at_dayofweek").alias("max_v"))
)
