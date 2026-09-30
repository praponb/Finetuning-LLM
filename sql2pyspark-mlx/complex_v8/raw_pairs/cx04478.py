from pyspark.sql import functions as F

recent_tickets = (
    df_tickets
    .filter((F.array_contains(F.col("tags"), "outage")) | (F.month(F.col("opened_at")).isin(2, 3)))
)

filtered = (
    recent_tickets.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_at") < F.lit("2025-02-18 11:00:00").cast("timestamp")), "left_anti")
)

labeled = (
    filtered
    .filter(F.col("agent_id") <= 9)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        "csat",
        F.hour(F.col("opened_at")).alias("opened_at_hour"),
    )
)

tagged = (
    labeled
    .filter(F.col("channel").isin("web", "email", "chat"))
    .select(
        "*",
        F.concat(F.col("channel"), F.lit("#"), F.col("csat").cast("string")).alias("channel_ref"),
        F.coalesce(F.col("opened_at_hour"), F.lit(0)).alias("opened_at_hour_filled"),
    )
)

result = (
    tagged
    .select("id", "csat", "opened_at_hour")
    .unpivot(
        ids=["id"],
        values=["csat", "opened_at_hour"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .orderBy("id", "measure")
)
