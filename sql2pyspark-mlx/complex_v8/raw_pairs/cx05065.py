from pyspark.sql import functions as F

base_events = (
    df_ticket_events
    .filter(F.month(F.col("event_at")).isin(2, 3))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

event_side = (
    base_events
    .filter((F.to_date(F.col("event_at")) < F.lit("2025-02-18").cast("date")) & (~F.col("actor").isin("customer", "system")))
    .select("id")
)

ticket_rows = df_tickets.select(F.col("agent_id").alias("id"))

result = (
    event_side
    .subtract(ticket_rows)
    .orderBy("id")
)
