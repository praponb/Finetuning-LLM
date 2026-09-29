from pyspark.sql import functions as F

base = (
    df_tickets
    .filter(F.col("priority").isNotNull())
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
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
        F.size(F.col("tags")).alias("tags_count"),
    )
)

qualified = (
    base
    .crossJoin(base.agg(F.avg("requester_id").alias("overall_avg_requester_id")))
    .filter(F.col("requester_id") > F.col("overall_avg_requester_id"))
    .drop("overall_avg_requester_id")
)

detail_rows = (
    qualified.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.priority_slug",
        "s.tags_count",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_at",
        "t.event_type",
    )
)

result = (
    detail_rows
    .select(
        "id",
        "event_id",
        "requester_id",
        "csat",
    )
    .unpivot(
        ids=["id", "event_id"],
        values=["requester_id", "csat"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "event_id", "field")
)
