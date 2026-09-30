from pyspark.sql import functions as F

in_window = (
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

agent_id_stats = (
    in_window
    .groupBy("agent_id")
    .agg(F.avg("prior_csat_3d").alias("avg_prior_csat_3d"))
)

vs_group = (
    in_window.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.prior_csat_3d") > F.col("g.avg_prior_csat_3d"))
    .select(
        "s.*",
        "g.avg_prior_csat_3d",
        F.round(F.col("s.prior_csat_3d") - F.col("g.avg_prior_csat_3d"), 2).alias("prior_csat_3d_minus_avg"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count(F.when(F.col("priority") != "P3", 1)).alias("ticket_priority_count"),
        F.count("*").alias("ticket_count"),
    )
)

summarized_rows = (
    vs_group.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_priority_count"), F.lit(0)).alias("ticket_priority_count"),
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

result = (
    summarized_rows
    .select(
        "id",
        "opened_at",
        "prior_rows_3d",
        "prior_csat_3d",
        "avg_prior_csat_3d",
        "ticket_priority_count",
        "ticket_count",
    )
    .orderBy(F.col("opened_at").desc_nulls_first(), "id")
    .limit(5)
)
