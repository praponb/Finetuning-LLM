from pyspark.sql import functions as F

joined_ranges = (
    df_tickets.alias("a")
    .join(
        df_tickets.alias("b"),
        (F.col("b.agent_id") == F.col("a.agent_id"))
        & (F.col("b.opened_at") < F.col("a.opened_at"))
        & (F.col("b.opened_at") >= F.col("a.opened_at") - F.expr("INTERVAL 3 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.agent_id", "a.opened_at")
    .agg(
        F.count("b.id").alias("prior_rows_3d"),
        F.sum("b.csat").alias("prior_csat_3d"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count(F.when(F.col("priority").isin("P3", "P2", "P1"), 1)).alias("ticket_priority_count"),
        F.max("csat").alias("max_ticket_csat"),
        F.count("*").alias("ticket_count"),
    )
)

profiled = (
    joined_ranges.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_priority_count"), F.lit(0)).alias("ticket_priority_count"),
        "t.max_ticket_csat",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

result = (
    profiled
    .select(
        "id",
        "agent_id",
        "opened_at",
        "prior_rows_3d",
        "ticket_priority_count",
        "max_ticket_csat",
        "ticket_count",
        F.abs(F.col("prior_csat_3d") - 11).alias("prior_csat_3d_distance"),
    )
    .orderBy(F.col("opened_at").desc_nulls_first(), F.col("agent_id").desc())
)
