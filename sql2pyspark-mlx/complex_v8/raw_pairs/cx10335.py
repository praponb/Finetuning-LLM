from pyspark.sql import functions as F

cleaned = df_tickets.filter((F.col("requester_id") >= 8) & (F.col("agent_id").isNotNull()))

tagged = (
    cleaned
    .filter(F.col("priority") == "P1")
    .select(
        "id",
        "requester_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "tags",
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
        F.upper(F.trim(F.col("priority"))).alias("priority_clean"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.count("*").alias("event_count"), F.count("*").alias("event_count_2"))
)

with_counts = (
    tagged.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        F.coalesce(F.col("t.event_count_2"), F.lit(0)).alias("event_count_2"),
    )
)

result = (
    with_counts
    .groupBy("csat")
    .pivot("channel", ["email", "web"])
    .agg(F.count("requester_id"))
    .orderBy("csat")
)
