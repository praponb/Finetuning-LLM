from pyspark.sql import functions as F

cleaned_events = (
    df_ticket_events
    .filter((F.col("actor") == "system") & (F.col("event_type").isin("created", "resolved", "reopened")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        F.concat_ws("-", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
        F.concat(F.col("actor"), F.lit("#"), F.col("id").cast("string")).alias("actor_ref"),
    )
)

enriched = (
    cleaned_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.*",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.agent_id").alias("ticket_agent_id"),
    )
)

stats = (
    enriched
    .groupBy("event_type")
    .agg(
        F.sum(F.when(F.col("ticket_priority").isNotNull(), F.col("ticket_requester_id")).otherwise(0)).alias("ticket_requester_id_ticket_priority_is_not_null"),
        F.sum(F.when(F.col("event_type") == "escalated", F.col("ticket_requester_id")).otherwise(0)).alias("ticket_requester_id_event_type_escalated"),
        F.count("*").alias("n_rows"),
        F.max("event_at").alias("last_event_at"),
        F.round(F.avg("ticket_requester_id"), 2).alias("avg_ticket_requester_id"),
    )
)

result = (
    stats
    .select(
        "event_type",
        "ticket_requester_id_ticket_priority_is_not_null",
        "ticket_requester_id_event_type_escalated",
        "n_rows",
        (F.floor(F.col("n_rows") / 2) * 2).alias("n_rows_bucket"),
        F.lower(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
    .orderBy(F.col("n_rows").asc())
)
