from pyspark.sql import functions as F

cleaned_tickets = (
    df_tickets
    .filter(F.hour(F.col("opened_at")).between(9, 12))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
    )
)

agent_id_stats = (
    cleaned_tickets
    .groupBy("agent_id")
    .agg(F.percentile_approx("csat", 0.5).alias("median_csat"))
)

compared = (
    cleaned_tickets.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.csat") >= F.col("g.median_csat"))
    .select(
        "s.*",
        "g.median_csat",
        F.round(F.col("s.csat") - F.col("g.median_csat"), 2).alias("csat_minus_median"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count("*").alias("ticket_count"),
        F.count("*").alias("ticket_count_2"),
        F.min("csat").alias("min_ticket_csat"),
    )
)

summarized_rows = (
    compared.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        F.coalesce(F.col("t.ticket_count_2"), F.lit(0)).alias("ticket_count_2"),
        "t.min_ticket_csat",
    )
)

rollup_base = (
    summarized_rows
    .groupBy(
        F.when(F.col("requester_id") >= 18, "A").when(F.col("requester_id") >= 9, "B").otherwise("C").alias("requester_id_tier"),
        "priority",
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("channel").alias("distinct_channels"),
        F.sum("csat_minus_median").alias("sum_csat_minus_median_check"),
    )
    .filter(F.col("sum_csat_minus_median_check") > -1.5)
    .drop("sum_csat_minus_median_check")
)

result = (
    rollup_base
    .select(
        "requester_id_tier",
        "priority",
        F.col("n_rows").alias("n_rows_v"),
        F.col("distinct_channels").alias("distinct_channels_v"),
    )
    .unpivot(
        ids=["requester_id_tier", "priority"],
        values=["n_rows_v", "distinct_channels_v"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .orderBy("requester_id_tier", "priority", "field")
)
