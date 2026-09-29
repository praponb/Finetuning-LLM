from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_prior_rows_7d_by_pr = Window.partitionBy("agent_id", "prior_rows_7d").orderBy("prior_rows_7d_minus_max")
w_opened_at_to_date_agent_id_b = Window.partitionBy(F.to_date("opened_at"), "agent_id").orderBy("prior_requester_id_7d", "id")
w_agent_id_prior_rows_7d_by_pr_2 = Window.partitionBy("agent_id", "prior_rows_7d").orderBy("prior_rows_7d_minus_max", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_prior_rows_7d = Window.partitionBy("agent_id", "prior_rows_7d")

in_window = (
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
    in_window
    .groupBy("agent_id")
    .agg(F.max("prior_rows_7d").alias("max_prior_rows_7d"))
)

above_average = (
    in_window.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.prior_rows_7d") == F.col("g.max_prior_rows_7d"))
    .select(
        "s.*",
        "g.max_prior_rows_7d",
        F.round(F.col("s.prior_rows_7d") - F.col("g.max_prior_rows_7d"), 2).alias("prior_rows_7d_minus_max"),
    )
)

with_windows = (
    above_average
    .select(
        "id",
        "opened_at",
        "prior_rows_7d",
        "max_prior_rows_7d",
        "prior_rows_7d_minus_max",
        F.dense_rank().over(w_agent_id_prior_rows_7d_by_pr).alias("prior_rows_7d_minus_max_dense_rank"),
        F.round(F.cume_dist().over(w_opened_at_to_date_agent_id_b), 3).alias("prior_requester_id_7d_cume_dist"),
        F.sum("prior_requester_id_7d").over(w_agent_id_prior_rows_7d_by_pr_2).alias("running_prior_requester_id_7d"),
        F.count(F.lit(1)).over(w_agent_id_prior_rows_7d).alias("rows_in_agent_id_prior_rows_7d"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "prior_rows_7d",
        "rows_in_agent_id_prior_rows_7d",
        "running_prior_requester_id_7d",
        "prior_rows_7d_minus_max_dense_rank",
    )
    .unpivot(
        ids=["id"],
        values=["prior_rows_7d", "rows_in_agent_id_prior_rows_7d", "running_prior_requester_id_7d", "prior_rows_7d_minus_max_dense_rank"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .orderBy("id", "metric")
)
