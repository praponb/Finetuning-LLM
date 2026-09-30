from pyspark.sql import functions as F

prepared_tickets = (
    df_tickets
    .filter((F.col("closed_at") >= F.lit("2025-01-14 23:00:00").cast("timestamp")) & (F.col("csat") > 2))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.date_format(F.col("opened_at"), "yyyy-MM-dd HH:00").alias("opened_at_label"),
    )
)

flattened = (
    prepared_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "opened_at_label",
        F.explode_outer("tags").alias("tag"),
    )
)

with_activity = (
    flattened
    .crossJoin(flattened.agg(F.avg("csat").alias("overall_avg_csat")))
    .filter(F.col("csat") > F.col("overall_avg_csat"))
    .drop("overall_avg_csat")
)

tagged = (
    with_activity
    .withColumn("priority_or_unknown", F.coalesce(F.col("priority"), F.lit("UNKNOWN")))
)

line_level_events = (
    tagged.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_type").isin("assigned", "reopened")), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.priority",
        "s.opened_at_label",
        "s.tag",
        "s.priority_or_unknown",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        F.col("t.actor").alias("event_actor"),
    )
)

summary = (
    line_level_events
    .groupBy("priority_or_unknown", "event_type")
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("requester_id", 0.5).alias("median_requester_id"),
        F.count(F.when(F.col("event_actor") == "system", 1)).alias("n_event_actor_system"),
        F.sum("requester_id").alias("sum_requester_id_check"),
    )
    .filter(F.col("sum_requester_id_check") > 7)
    .drop("sum_requester_id_check")
)

result = (
    summary
    .groupBy("row_count")
    .pivot("event_type", ["replied", "escalated", "resolved"])
    .agg(F.sum("median_requester_id").alias("amount"), F.count("median_requester_id").alias("n"))
    .orderBy("row_count")
)
