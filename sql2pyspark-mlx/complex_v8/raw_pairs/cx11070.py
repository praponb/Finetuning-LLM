from pyspark.sql import functions as F

joined_ranges = (
    df_tickets.alias("a")
    .join(
        df_tickets.alias("b"),
        (F.col("b.agent_id") == F.col("a.agent_id"))
        & (F.col("b.opened_at") < F.col("a.opened_at"))
        & (F.col("b.opened_at") >= F.col("a.opened_at") - F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.agent_id", "a.opened_at")
    .agg(
        F.count("b.id").alias("prior_rows_7d"),
        F.sum("b.csat").alias("prior_csat_7d"),
    )
)

joined_agent = (
    joined_ranges.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.prior_rows_7d",
        "s.prior_csat_7d",
        F.col("su.tier").alias("agent_tier"),
    )
)

grouped = (
    joined_agent
    .groupBy(F.date_format("opened_at", "yyyy-MM").alias("opened_at_ym"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.stddev("prior_rows_7d"), 2).alias("stddev_prior_rows_7d"),
        F.count(F.when(F.col("agent_tier") == "T2", 1)).alias("n_agent_tier_t2"),
    )
)

result = (
    grouped
    .select(
        "opened_at_ym",
        "distinct_ids",
        "stddev_prior_rows_7d",
        "n_agent_tier_t2",
        F.round(F.col("stddev_prior_rows_7d") * 1.5, 2).alias("stddev_prior_rows_7d_adj"),
        (F.floor(F.col("distinct_ids") / 2) * 2).alias("distinct_ids_bucket"),
    )
    .orderBy("n_agent_tier_t2")
)
