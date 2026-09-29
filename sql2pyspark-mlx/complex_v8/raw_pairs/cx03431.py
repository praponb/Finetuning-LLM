from pyspark.sql import functions as F

covered = (
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

labeled = (
    covered
    .filter(F.col("prior_csat_7d") > 5)
    .select(
        "id",
        "agent_id",
        "prior_rows_7d",
        "prior_csat_7d",
        F.least(F.col("prior_csat_7d"), F.col("prior_rows_7d")).alias("least_prior_csat_7d_prior_rows_7d"),
        F.abs(F.col("prior_csat_7d") - 14).alias("prior_csat_7d_distance"),
        F.date_format(F.col("opened_at"), "yyyy-MM").alias("opened_at_label"),
    )
)

result = (
    labeled
    .filter(F.col("prior_csat_7d") < 16)
    .select("id", "prior_rows_7d")
    .orderBy(F.col("prior_rows_7d").desc(), F.col("id").desc())
)
