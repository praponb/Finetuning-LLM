from pyspark.sql import functions as F

cleaned_tickets = (
    df_tickets
    .filter((F.col("opened_at") < F.lit("2025-03-08 10:00:00").cast("timestamp")) | (F.col("channel") == "phone"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.concat(F.col("priority"), F.lit("#"), F.col("agent_id").cast("string")).alias("priority_ref"),
    )
)

ticket_side = (
    cleaned_tickets
    .select(
        F.col("agent_id").alias("key_id"),
        F.col("priority_ref").alias("category"),
    )
)

event_side = df_ticket_events.select("id", "actor")

result = (
    ticket_side
    .subtract(event_side)
    .orderBy(F.col("category").desc())
)
