from pyspark.sql import functions as F

valid = (
    df_ticket_events
    .filter((F.hour(F.col("event_at")).between(8, 11)) & (F.col("event_type") == "resolved"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
    )
)

event_side = valid.select("event_type_slug", "event_type")

ticket_side = (
    df_tickets
    .select(
        F.col("channel").alias("event_type_slug"),
        F.col("priority").alias("event_type"),
    )
)

result = (
    event_side
    .union(ticket_side)
    .distinct()
    .orderBy("event_type")
)
