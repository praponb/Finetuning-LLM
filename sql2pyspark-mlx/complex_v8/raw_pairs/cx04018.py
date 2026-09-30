from pyspark.sql import functions as F

valid = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
)

event_side = (
    valid
    .select(
        F.col("event_type_len").alias("ref_id"),
        "ticket_id",
        F.col("event_at").alias("happened_at"),
    )
)

event_side_2 = (
    df_ticket_events
    .filter(F.col("event_type") == "assigned")
    .select("ticket_id", "id", "event_at")
)

result = (
    event_side
    .union(event_side_2)
    .orderBy(F.col("ticket_id").desc(), "ref_id")
)
