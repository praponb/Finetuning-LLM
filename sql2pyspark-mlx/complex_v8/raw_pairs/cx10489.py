from pyspark.sql import functions as F

scoped_events = (
    df_ticket_events
    .filter((F.col("actor").isin("system", "agent")) & (F.month(F.col("event_at")).isin(1, 3)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

screened = (
    scoped_events.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.hour(F.col("t.event_at")).between(6, 12)) & (~F.col("t.actor").isin("customer", "system")), "left_semi")
    .filter(F.month(F.col("event_at")).isin(1, 3))
)

result = (
    screened
    .select("id", "event_at", "event_type")
    .orderBy(F.col("id").asc_nulls_last(), F.col("event_at").desc())
)
