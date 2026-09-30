from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_opened_at_month = Window.partitionBy("agent_id", F.month("opened_at"))
w_all_by_prior_rows_3d_moving = Window.orderBy("prior_rows_3d", "id").rowsBetween(-2, Window.currentRow)
w_all_by_prior_rows_3d_ties = Window.orderBy("prior_rows_3d")
w_all_by_prior_rows_3d = Window.orderBy("prior_rows_3d", "id")

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

with_trends = (
    in_window
    .select(
        "*",
        F.round(F.col("prior_csat_3d") - F.avg("prior_csat_3d").over(w_agent_id_opened_at_month), 2).alias("prior_csat_3d_vs_agent_id_opened_at_month_avg"),
        F.round(F.avg("prior_rows_3d").over(w_all_by_prior_rows_3d_moving), 2).alias("prior_rows_3d_avg_3"),
        F.dense_rank().over(w_all_by_prior_rows_3d_ties).alias("prior_rows_3d_dense_rank"),
        F.lag("prior_csat_3d", 1, 0).over(w_all_by_prior_rows_3d).alias("prev_prior_csat_3d"),
        F.round(F.col("prior_rows_3d") - F.avg("prior_rows_3d").over(w_agent_id_opened_at_month), 2).alias("prior_rows_3d_vs_agent_id_opened_at_month_avg"),
        F.round(F.col("prior_rows_3d") / F.nullif(F.sum("prior_rows_3d").over(w_agent_id_opened_at_month), F.lit(0)), 4).alias("prior_rows_3d_share_of_agent_id_opened_at_month"),
    )
)

result = (
    with_trends
    .filter(F.col("prior_rows_3d_vs_agent_id_opened_at_month_avg") <= 1.6)
    .select(
        "id",
        "agent_id",
        "opened_at",
        "prior_csat_3d",
        "prior_csat_3d_vs_agent_id_opened_at_month_avg",
        "prior_rows_3d_avg_3",
        "prior_rows_3d_dense_rank",
        "prev_prior_csat_3d",
        "prior_rows_3d_vs_agent_id_opened_at_month_avg",
        "prior_rows_3d_share_of_agent_id_opened_at_month",
    )
    .orderBy("prior_csat_3d_vs_agent_id_opened_at_month_avg")
)
