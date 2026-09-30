from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("agent_id").orderBy(F.col("prior_requester_id_7d_minus_median").desc(), F.col("id").desc())

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
        F.sum("b.requester_id").alias("prior_requester_id_7d"),
    )
)

agent_id_stats = (
    covered
    .groupBy("agent_id")
    .agg(F.percentile_approx("prior_requester_id_7d", 0.5).alias("median_prior_requester_id_7d"))
)

above_average = (
    covered.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.prior_requester_id_7d") >= F.col("g.median_prior_requester_id_7d"))
    .select(
        "s.*",
        "g.median_prior_requester_id_7d",
        F.round(F.col("s.prior_requester_id_7d") - F.col("g.median_prior_requester_id_7d"), 2).alias("prior_requester_id_7d_minus_median"),
    )
)

with_details = (
    above_average.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (~F.col("su.team").isin("Billing", "Tech")), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.prior_rows_7d",
        "s.prior_requester_id_7d",
        "s.median_prior_requester_id_7d",
        "s.prior_requester_id_7d_minus_median",
        "su.agent_name",
    )
)

unique_rows = (
    with_details
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    unique_rows
    .select(
        "id",
        "agent_id",
        "opened_at",
        "prior_rows_7d",
        "median_prior_requester_id_7d",
        "prior_requester_id_7d_minus_median",
        "agent_name",
        F.to_date(F.col("opened_at")).alias("opened_at_date"),
        (F.floor(F.col("prior_requester_id_7d") / 25) * 25).alias("prior_requester_id_7d_bucket"),
    )
    .orderBy(F.col("median_prior_requester_id_7d").asc_nulls_last())
)
