from pyspark.sql import functions as F

cleaned = (
    df_ticket_events
    .filter(F.col("ticket_id") <= 38)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.dayofweek(F.col("event_at")).alias("event_at_dayofweek"),
    )
)

event_side = (
    cleaned
    .filter(F.col("event_at_dayofweek") > 3)
    .select(
        F.lit("event").alias("origin"),
        F.col("ticket_id").alias("qty"),
        "event_at",
    )
)

ticket_rows = (
    df_tickets
    .filter(F.col("priority").isin("P3", "P2", "P1"))
    .select(F.lit("ticket").alias("origin"), "csat", "opened_at")
)

result = (
    event_side
    .union(ticket_rows)
    .distinct()
    .orderBy("origin")
)
