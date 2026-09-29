from pyspark.sql import functions as F

valid_tickets = (
    df_tickets
    .filter(F.col("requester_id") > 9)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "channel",
        "tags",
        F.coalesce(F.col("requester_id"), F.lit(0)).alias("requester_id_filled"),
        F.abs(F.col("csat") - 3).alias("csat_distance"),
    )
)

with_activity = (
    valid_tickets.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (~F.col("t.actor").isin("customer", "agent")), "left_semi")
)

qualified = (
    with_activity.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_type").isin("reopened", "assigned", "escalated")) & (F.col("t.event_at") < F.lit("2025-03-02 09:00:00").cast("timestamp")), "left_anti")
)

result = (
    qualified
    .groupingSets(
        [["channel", "priority"], ["channel"], []],
        "channel", "priority",
    )
    .agg(
        F.array_sort(F.collect_set("priority")).alias("priority_list"),
        F.min("csat_distance").alias("min_csat_distance"),
        F.sum("requester_id").alias("total_requester_id"),
    )
)
