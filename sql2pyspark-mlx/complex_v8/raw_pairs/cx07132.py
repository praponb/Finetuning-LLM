from pyspark.sql import functions as F

ticket_side = (
    df_tickets
    .filter((F.col("priority") != "P3") & (F.col("channel") != "email"))
    .select(
        F.lit("ticket").alias("record_type"),
        F.col("id").alias("qty"),
        "channel",
        "agent_id",
    )
)

event_rows = (
    df_ticket_events
    .select(
        F.lit("event").alias("record_type"),
        "id",
        "event_type",
        "ticket_id",
    )
)

result = (
    ticket_side
    .union(event_rows)
    .orderBy(F.col("channel").desc())
)
