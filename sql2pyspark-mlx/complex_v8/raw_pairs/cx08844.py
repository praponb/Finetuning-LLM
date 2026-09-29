from pyspark.sql import functions as F

base = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM-dd").alias("event_at_label"),
    )
)

qualified = (
    base.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type").isin("replied", "assigned")) & (F.col("t.actor").isin("customer", "agent")), "left_anti")
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.max("event_at").alias("last_event_at_2"),
    )
)

profiled = (
    qualified.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.last_event_at", "t.last_event_at_2")
)

result = (
    profiled
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "last_event_at",
        F.date_trunc("day", F.col("event_at")).alias("event_at_day"),
        F.concat(F.col("actor"), F.lit("#"), F.col("ticket_id").cast("string")).alias("actor_ref"),
    )
    .orderBy(F.col("last_event_at").desc(), F.col("actor_ref").desc())
)
