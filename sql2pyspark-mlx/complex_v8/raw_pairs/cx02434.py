from pyspark.sql import functions as F

prepared_events = (
    df_ticket_events
    .filter((~F.col("actor").isin("agent", "customer")) | (F.col("event_at") >= F.lit("2025-02-06 11:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

filtered = (
    prepared_events.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_at") < F.lit("2025-02-12 08:00:00").cast("timestamp")), "left_anti")
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
    filtered.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.last_event_at", "t.last_event_at_2")
)

tagged = (
    profiled
    .select(
        "id",
        "event_at",
        "last_event_at_2",
        F.date_format(F.col("event_at"), "yyyy-MM-dd").alias("event_at_label"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_type_ref"),
    )
)

result = (
    tagged
    .select(
        "id",
        "event_at",
        "last_event_at_2",
        "event_at_label",
        "event_type_ref",
    )
    .orderBy("last_event_at_2")
)
