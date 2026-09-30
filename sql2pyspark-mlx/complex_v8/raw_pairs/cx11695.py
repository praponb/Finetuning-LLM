from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_at_month_prior_rows_7 = Window.partitionBy(F.month("opened_at"), "prior_rows_7d").orderBy("prior_rows_7d", "id").rowsBetween(-6, Window.currentRow)
w_opened_at_month_prior_rows_7_2 = Window.partitionBy(F.month("opened_at"), "prior_rows_7d").orderBy("prior_rows_7d", "id")
w_agent_id = Window.partitionBy("agent_id")
w_prior_rows_7d_agent_id_by_pr = Window.partitionBy("prior_rows_7d", "agent_id").orderBy("prior_rows_7d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_prior_rows_7d_agent_id_by_pr_2 = Window.partitionBy("prior_rows_7d", "agent_id").orderBy("prior_rows_7d", "id")

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

ranked = (
    in_window
    .select(
        "*",
        F.round(F.avg("prior_requester_id_7d").over(w_opened_at_month_prior_rows_7), 2).alias("prior_requester_id_7d_avg_7"),
        (F.col("prior_requester_id_7d") - F.lag("prior_requester_id_7d").over(w_opened_at_month_prior_rows_7_2)).alias("prior_requester_id_7d_change"),
        F.max("prior_rows_7d").over(w_agent_id).alias("agent_id_max_prior_rows_7d"),
        F.sum("prior_rows_7d").over(w_prior_rows_7d_agent_id_by_pr).alias("running_prior_rows_7d"),
        F.lag("prior_requester_id_7d", 2).over(w_prior_rows_7d_agent_id_by_pr_2).alias("prev2_prior_requester_id_7d"),
        F.lag("opened_at", 3).over(w_opened_at_month_prior_rows_7_2).alias("prev3_opened_at"),
    )
)

with_activity = (
    ranked.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.length(F.trim(F.col("su.agent_name"))) >= 7), "left_semi")
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.round(F.avg("requester_id"), 2).alias("avg_ticket_requester_id"))
)

summarized_rows = (
    with_activity.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select("s.*", "t.avg_ticket_requester_id")
)

result = (
    summarized_rows
    .groupingSets(
        [["agent_id", "prior_rows_7d"], ["agent_id"], ["prior_rows_7d"]],
        "agent_id", "prior_rows_7d",
    )
    .agg(
        F.grouping("agent_id").alias("is_total_agent_id"),
        F.min("prior_requester_id_7d_avg_7").alias("min_prior_requester_id_7d_avg_7"),
    )
    .orderBy(
        F.col("agent_id").asc_nulls_last(),
        F.col("prior_rows_7d").asc_nulls_last(),
    )
)
