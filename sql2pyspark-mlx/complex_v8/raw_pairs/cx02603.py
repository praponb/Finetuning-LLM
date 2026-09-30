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

rollup_base = (
    joined_ranges
    .groupBy(
        "prior_rows_3d",
        F.date_trunc("month", "opened_at").alias("opened_at_month"),
    )
    .agg(
        F.round(F.stddev("prior_rows_3d"), 2).alias("stddev_prior_rows_3d"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    rollup_base
    .cube("prior_rows_3d", "distinct_ids")
    .agg(
        F.max("opened_at_month").alias("last_opened_at_month"),
        F.count("*").alias("cnt"),
    )
    .orderBy(
        F.col("prior_rows_3d").asc_nulls_last(),
        F.col("distinct_ids").asc_nulls_last(),
    )
)
