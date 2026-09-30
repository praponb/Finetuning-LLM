from pyspark.sql import functions as F

prepared = (
    df_tickets
    .filter(F.col("agent_id") <= 4)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "tags",
        F.date_format(F.col("opened_at"), "yyyy-MM-dd").alias("opened_at_label"),
    )
)

exploded_detail_events = (
    prepared.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp")), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.channel",
        "s.tags",
        "s.opened_at_label",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_type",
        "t.event_at",
    )
)

tagged = (
    exploded_detail_events
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "tags",
        "opened_at_label",
        "event_id",
        "event_actor",
        "event_type",
        F.coalesce(F.col("requester_id"), F.lit(0)).alias("requester_id_filled"),
        F.array_join(F.array_sort(F.col("tags")), ",").alias("tags_text"),
    )
)

qualified = (
    tagged.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.closed_at") < F.lit("2025-03-09 04:00:00").cast("timestamp")) & (~F.col("t.priority").isin("P4", "P1")), "left_semi")
    .filter(F.col("closed_at") < F.lit("2025-02-10 04:00:00").cast("timestamp"))
)

result = (
    qualified
    .select(
        "id",
        "event_id",
        "requester_id_filled",
        "requester_id",
    )
    .unpivot(
        ids=["id", "event_id"],
        values=["requester_id_filled", "requester_id"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .orderBy("id", "event_id", "field")
)
