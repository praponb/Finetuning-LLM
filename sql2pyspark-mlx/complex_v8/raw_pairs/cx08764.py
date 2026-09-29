from pyspark.sql import functions as F

in_window = (
    df_tickets.alias("a")
    .join(
        df_tickets.alias("b"),
        (F.col("b.agent_id") == F.col("a.agent_id"))
        & (F.col("b.opened_at") < F.col("a.opened_at"))
        & (F.col("b.opened_at") >= F.col("a.opened_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.agent_id", "a.opened_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.requester_id").alias("prior_requester_id_12h"),
    )
)

grouped = (
    in_window
    .groupBy("agent_id", "prior_rows_12h")
    .agg(
        F.min("opened_at").alias("first_opened_at"),
        F.count("*").alias("cnt"),
        F.countDistinct("agent_id").alias("distinct_agent_ids"),
        F.sum("prior_requester_id_12h").alias("total_prior_requester_id_12h"),
        F.round(F.sum("prior_rows_12h") / F.nullif(F.sum("prior_requester_id_12h"), F.lit(0)), 3).alias("prior_rows_12h_to_prior_requester_id_12h_ratio"),
        F.round(F.stddev("prior_requester_id_12h"), 2).alias("stddev_prior_requester_id_12h"),
        F.avg("prior_rows_12h").alias("avg_prior_rows_12h_check"),
    )
    .filter(F.col("avg_prior_rows_12h_check") > 3)
    .drop("avg_prior_rows_12h_check")
)

result = (
    grouped
    .filter(F.col("prior_rows_12h") > 1)
    .select(
        "agent_id",
        "prior_rows_12h",
        "first_opened_at",
        "distinct_agent_ids",
        "total_prior_requester_id_12h",
        "stddev_prior_requester_id_12h",
        F.coalesce(F.col("cnt"), F.lit(0)).alias("cnt_filled"),
    )
    .orderBy(F.col("total_prior_requester_id_12h").asc_nulls_last())
)
