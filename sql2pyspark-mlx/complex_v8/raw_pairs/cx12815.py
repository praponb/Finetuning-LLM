from pyspark.sql import functions as F

scoped_events = (
    df_ticket_events
    .filter(F.col("actor") != "system")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("actor"), F.lit("#"), F.col("id").cast("string")).alias("actor_ref"),
    )
)

event_side = scoped_events.select("event_type", F.col("actor").alias("name"), "ticket_id")

ticket_rows = (
    df_tickets
    .filter((F.col("priority").isin("P1", "P4")) & (F.col("opened_at") >= F.lit("2025-01-13 13:00:00").cast("timestamp")))
    .select("priority", "channel", "agent_id")
)

result = (
    event_side
    .union(ticket_rows)
    .orderBy(F.col("name").desc(), "ticket_id")
)
