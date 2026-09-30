from pyspark.sql import functions as F

in_window = (
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
        F.sum("b.requester_id").alias("prior_requester_id_1d"),
    )
)

enriched = (
    in_window.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.prior_rows_1d",
        "s.prior_requester_id_1d",
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
    )
)

annotated = (
    enriched
    .select(
        "id",
        "agent_id",
        "opened_at",
        "prior_rows_1d",
        "agent_hired_on",
        (F.floor(F.col("prior_requester_id_1d") / 25) * 25).alias("prior_requester_id_1d_bucket"),
        F.substring(F.trim(F.col("agent_name")), 1, 2).alias("agent_name_prefix"),
    )
)

per_group = (
    annotated
    .groupBy("agent_id")
    .agg(
        F.round(F.stddev("prior_rows_1d"), 2).alias("stddev_prior_rows_1d"),
        F.min("agent_hired_on").alias("first_agent_hired_on"),
        F.countDistinct("agent_id").alias("distinct_agent_ids"),
        F.max("prior_rows_1d").alias("max_prior_rows_1d"),
    )
    .filter(F.col("max_prior_rows_1d") > 1)
)

result = (
    per_group
    .rollup("max_prior_rows_1d", "agent_id")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.avg("stddev_prior_rows_1d"), 2).alias("avg_stddev_prior_rows_1d"),
        F.count("*").alias("cnt"),
        F.sum("max_prior_rows_1d").alias("total_max_prior_rows_1d"),
    )
)
