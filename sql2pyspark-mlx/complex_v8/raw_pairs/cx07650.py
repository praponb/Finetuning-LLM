from pyspark.sql import functions as F

joined_ranges = (
    df_tickets.alias("a")
    .join(
        df_tickets.alias("b"),
        (F.col("b.agent_id") == F.col("a.agent_id"))
        & (F.col("b.opened_at") < F.col("a.opened_at"))
        & (F.col("b.opened_at") >= F.col("a.opened_at") - F.expr("INTERVAL 1 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.agent_id", "a.opened_at")
    .agg(
        F.count("b.id").alias("prior_rows_1d"),
        F.sum("b.csat").alias("prior_csat_1d"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count(F.when(~F.col("channel").isin("phone", "web"), 1)).alias("ticket_channel_count"),
        F.count("*").alias("ticket_count"),
        F.max("closed_at").alias("last_ticket_closed_at"),
    )
)

with_counts = (
    joined_ranges.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_channel_count"), F.lit(0)).alias("ticket_channel_count"),
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        "t.last_ticket_closed_at",
    )
)

result = (
    with_counts
    .filter(F.col("prior_rows_1d") < 6)
    .select(
        "id",
        "opened_at",
        "prior_rows_1d",
        "ticket_channel_count",
        "ticket_count",
        "last_ticket_closed_at",
        F.date_format(F.col("opened_at"), "MMM yyyy").alias("opened_at_label"),
        F.when(F.col("prior_rows_1d") >= 6, "A").when(F.col("prior_rows_1d") >= 3, "B").otherwise("C").alias("prior_rows_1d_tier"),
    )
)
