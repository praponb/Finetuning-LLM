from pyspark.sql import functions as F

prepared = (
    df_tickets
    .filter((F.col("opened_at") < F.lit("2025-02-18 07:00:00").cast("timestamp")) & (F.col("requester_id").between(7, 25)))
)

scored = (
    prepared
    .filter(F.col("closed_at") >= F.lit("2025-02-02 03:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "closed_at",
        "priority",
        "channel",
        "tags",
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
        (F.col("opened_at") + F.expr("INTERVAL 2 HOURS")).alias("opened_at_plus_2h"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.max("event_at").alias("last_event_at"))
)

with_counts = (
    scored.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select("s.*", "t.last_event_at")
)

without_matches = (
    with_counts.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.actor").isin("customer", "system")), "left_anti")
)

result = (
    without_matches
    .select(
        "id",
        F.col("channel_len").alias("channel_len_v"),
        F.col("requester_id").alias("requester_id_v"),
    )
    .unpivot(
        ids=["id"],
        values=["channel_len_v", "requester_id_v"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "metric_name")
)
