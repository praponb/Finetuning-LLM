from pyspark.sql import functions as F

filtered = (
    df_tickets
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "tags",
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.sum("requester_id").alias("total_ticket_requester_id"),
        F.count(F.when(F.col("priority").isin("P2", "P4", "P1"), 1)).alias("ticket_priority_count"),
        F.max("closed_at").alias("last_ticket_closed_at"),
    )
)

with_stats = (
    filtered.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "t.total_ticket_requester_id",
        F.coalesce(F.col("t.ticket_priority_count"), F.lit(0)).alias("ticket_priority_count"),
        "t.last_ticket_closed_at",
    )
)

tagged = (
    with_stats
    .select(
        "*",
        F.year(F.col("closed_at")).alias("closed_at_year"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("last_ticket_closed_at")).alias("days_since_last_ticket_closed_at"),
    )
)

result = (
    tagged
    .groupingSets(
        [["agent_id", "ticket_priority_count", "closed_at_year"], ["agent_id"]],
        "agent_id", "ticket_priority_count", "closed_at_year",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(100 * F.count(F.when(F.col("priority").isin("P1", "P4", "P3"), 1)) / F.count("*"), 1).alias("pct_priority_in_p1_p4_p3"),
    )
    .orderBy(
        F.col("agent_id").asc_nulls_last(),
        F.col("ticket_priority_count").asc_nulls_last(),
        F.col("closed_at_year").asc_nulls_last(),
    )
)
