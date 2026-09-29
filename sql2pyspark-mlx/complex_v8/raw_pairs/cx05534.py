from pyspark.sql import functions as F

valid = (
    df_tickets
    .filter(F.col("csat") < 5)
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "csat",
        "tags",
        F.abs(F.col("requester_id") - 18).alias("requester_id_distance"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.max("event_at").alias("last_event_at"))
)

with_counts = (
    valid.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select("s.*", "t.last_event_at")
)

result = (
    with_counts
    .select(
        "id",
        F.col("csat").alias("csat_v"),
        F.col("requester_id_distance").alias("m_requester_id_distance"),
    )
    .unpivot(
        ids=["id"],
        values=["csat_v", "m_requester_id_distance"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "field")
)
