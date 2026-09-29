from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_opened_at_running = Window.orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_opened_at = Window.orderBy(F.col("opened_at").desc(), "id")
w_all_by_opened_at_full = Window.orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

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

with_details = (
    in_window.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.prior_requester_id_7d",
        F.col("su.team").alias("agent_team"),
    )
)

with_windows = (
    with_details
    .select(
        "id",
        "agent_id",
        "opened_at",
        "agent_team",
        F.max("prior_requester_id_7d").over(w_all_by_opened_at_running).alias("max_prior_requester_id_7d_so_far"),
        F.ntile(5).over(w_all_by_opened_at).alias("opened_at_quintile"),
        F.dense_rank().over(w_all_by_opened_at).alias("opened_at_dense_rank"),
        F.first("prior_requester_id_7d").over(w_all_by_opened_at_full).alias("first_prior_requester_id_7d"),
    )
)

result = (
    with_windows
    .groupingSets(
        [["agent_id", "opened_at_quintile"], ["agent_id"], ["opened_at_quintile"], []],
        "agent_id", "opened_at_quintile",
    )
    .agg(
        F.grouping("agent_id").alias("is_total_agent_id"),
        F.round(100 * F.count(F.when(F.col("agent_team") != "Tech", 1)) / F.count("*"), 1).alias("pct_agent_team_tech"),
    )
    .orderBy(
        F.col("agent_id").asc_nulls_last(),
        F.col("opened_at_quintile").asc_nulls_last(),
    )
)
