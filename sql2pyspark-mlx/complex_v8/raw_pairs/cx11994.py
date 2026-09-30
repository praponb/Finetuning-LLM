from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_prior_rows_3d_agent_id = Window.partitionBy("prior_rows_3d", "agent_id")
w_opened_at_month_by_opened_at = Window.partitionBy(F.month("opened_at")).orderBy(F.col("opened_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_opened_at_month_by_opened_at_2 = Window.partitionBy(F.month("opened_at")).orderBy(F.col("opened_at").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)

matched = (
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
        F.sum("b.requester_id").alias("prior_requester_id_3d"),
    )
)

sequenced = (
    matched
    .select(
        "*",
        F.count(F.lit(1)).over(w_prior_rows_3d_agent_id).alias("rows_in_prior_rows_3d_agent_id"),
        F.first("opened_at").over(w_opened_at_month_by_opened_at).alias("first_opened_at"),
        F.round(F.avg("prior_rows_3d").over(w_opened_at_month_by_opened_at_2), 2).alias("prior_rows_3d_avg_2"),
    )
)

agent_id_stats = (
    sequenced
    .groupBy("agent_id")
    .agg(F.percentile_approx("prior_requester_id_3d", 0.5).alias("median_prior_requester_id_3d"))
)

compared = (
    sequenced.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.prior_requester_id_3d") >= F.col("g.median_prior_requester_id_3d"))
    .select(
        "s.*",
        "g.median_prior_requester_id_3d",
        F.round(F.col("s.prior_requester_id_3d") - F.col("g.median_prior_requester_id_3d"), 2).alias("prior_requester_id_3d_minus_median"),
    )
)

result = (
    compared
    .select(
        "id",
        "agent_id",
        "opened_at",
        "prior_rows_3d",
        "prior_requester_id_3d",
        "rows_in_prior_rows_3d_agent_id",
        "first_opened_at",
        "prior_rows_3d_avg_2",
        "prior_requester_id_3d_minus_median",
        F.least(F.col("prior_requester_id_3d"), F.col("prior_rows_3d")).alias("least_prior_requester_id_3d_prior_rows_3d"),
        F.round(F.col("prior_requester_id_3d") / F.nullif(F.col("prior_rows_3d_avg_2"), F.lit(0)), 3).alias("prior_requester_id_3d_per_prior_rows_3d_avg_2"),
    )
    .orderBy(F.col("first_opened_at").asc_nulls_last())
)
