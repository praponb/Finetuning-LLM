from pyspark.sql import functions as F

prepared = (
    df_tickets
    .filter(~F.array_contains(F.col("tags"), "bug"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "tags",
    )
)

ticket_side = prepared.select(F.col("requester_id").alias("qty"), "opened_at", "csat")

event_side = (
    df_ticket_events
    .filter(F.col("event_type") == "assigned")
    .select("ticket_id", "event_at", "id")
)

combined = (
    ticket_side
    .union(event_side)
    .distinct()
)

result = (
    combined
    .groupBy("qty")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("csat").alias("total_csat"),
        F.max("opened_at").alias("latest_opened_at"),
    )
    .orderBy("qty")
)
