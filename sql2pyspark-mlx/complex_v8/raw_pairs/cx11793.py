from pyspark.sql import functions as F

base_tickets = (
    df_tickets
    .filter(F.array_contains(F.col("tags"), "outage"))
    .select(
        "id",
        "agent_id",
        "closed_at",
        "channel",
        "csat",
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
        F.concat(F.col("priority"), F.lit("#"), F.col("requester_id").cast("string")).alias("priority_ref"),
    )
)

stats = (
    base_tickets
    .groupBy("channel", "agent_id", "priority_or_unknown")
    .agg(
        F.round(F.stddev("csat"), 2).alias("stddev_csat"),
        F.min("csat").alias("min_csat"),
        F.count("*").alias("n_rows"),
        F.max("closed_at").alias("last_closed_at"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count("*").alias("ticket_count"),
        F.count("*").alias("ticket_count_2"),
        F.count(F.when(F.col("priority") == "P2", 1)).alias("ticket_priority_count"),
    )
)

summarized_rows = (
    stats.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        F.coalesce(F.col("t.ticket_count_2"), F.lit(0)).alias("ticket_count_2"),
        F.coalesce(F.col("t.ticket_priority_count"), F.lit(0)).alias("ticket_priority_count"),
    )
)

result = (
    summarized_rows
    .groupBy("last_closed_at")
    .pivot("priority_or_unknown", ["P1", "P3", "P2", "UNKNOWN"])
    .agg(F.count("ticket_count").alias("count_v"), F.max("ticket_count").alias("max_v"))
)
