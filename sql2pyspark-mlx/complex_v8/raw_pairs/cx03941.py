from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_team = Window.partitionBy("agent_team")
w_agent_team_by_requester_id_r = Window.partitionBy("agent_team").orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_requester_id = Window.orderBy(F.col("requester_id").desc(), "id")
w_agent_team_by_requester_id = Window.partitionBy("agent_team").orderBy(F.col("requester_id").desc(), "id")
w_agent_id_by_requester_id_den = Window.partitionBy("agent_id").orderBy(F.col("requester_id_dense_rank").desc())
w_agent_id_by_requester_id_den_2 = Window.partitionBy("agent_id").orderBy(F.col("requester_id_dense_rank").desc(), "id")
w_agent_team_requester_id_dens = Window.partitionBy("agent_team", "requester_id_dense_rank")
w_requester_id_dense_rank_by_r = Window.partitionBy("requester_id_dense_rank").orderBy("requester_id", "id")

cleaned = (
    df_tickets
    .filter((F.to_date(F.col("closed_at")) < F.lit("2025-03-04").cast("date")) & (F.col("csat") >= 2))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "csat",
        "tags",
        F.date_trunc("week", F.col("opened_at")).alias("opened_at_week"),
    )
)

joined_agent = (
    cleaned.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.tags",
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
    )
)

with_windows = (
    joined_agent
    .select(
        "*",
        F.min("requester_id").over(w_agent_team).alias("agent_team_min_requester_id"),
        F.size(F.collect_set("agent_id").over(w_agent_team)).alias("distinct_agent_id_in_agent_team"),
        F.min("requester_id").over(w_agent_team_by_requester_id_r).alias("min_requester_id_so_far"),
        F.lag("requester_id").over(w_all_by_requester_id).alias("prev_requester_id"),
        F.dense_rank().over(w_agent_team_by_requester_id).alias("requester_id_dense_rank"),
        F.sum("requester_id").over(w_agent_team).alias("agent_team_sum_requester_id"),
        F.count(F.lit(1)).over(w_agent_team).alias("rows_in_agent_team"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_agent_team), 2).alias("requester_id_vs_agent_team_avg"),
        F.sum("requester_id").over(w_agent_team_by_requester_id_r).alias("running_requester_id"),
    )
)

top_per_group = with_windows.filter(F.col("requester_id_dense_rank") == 1)

with_windows_2 = (
    top_per_group
    .select(
        "id",
        "requester_id",
        "agent_team",
        "min_requester_id_so_far",
        "prev_requester_id",
        "requester_id_dense_rank",
        "agent_team_sum_requester_id",
        "requester_id_vs_agent_team_avg",
        "running_requester_id",
        F.round(F.percent_rank().over(w_agent_id_by_requester_id_den), 3).alias("requester_id_dense_rank_percent_rank"),
        F.row_number().over(w_agent_id_by_requester_id_den_2).alias("requester_id_dense_rank_seq"),
        F.count(F.lit(1)).over(w_agent_team_requester_id_dens).alias("rows_in_agent_team_requester_id_dense_rank"),
        (F.col("requester_id_dense_rank") - F.lag("requester_id_dense_rank").over(w_requester_id_dense_rank_by_r)).alias("requester_id_dense_rank_change"),
        F.dense_rank().over(w_agent_id_by_requester_id_den).alias("requester_id_dense_rank_dense_rank"),
    )
)

stats = (
    with_windows_2
    .groupBy("requester_id_dense_rank", "requester_id_dense_rank_seq", "agent_team")
    .agg(
        F.sum("rows_in_agent_team_requester_id_dense_rank").alias("total_rows_in_agent_team_requester_id_dense_rank"),
        F.count(F.when(F.col("agent_team").isin("Billing", "Onboarding"), 1)).alias("n_agent_team_in_billing_onboar"),
        F.array_sort(F.collect_set("agent_team")).alias("agent_team_list"),
        F.sum(F.when(F.col("agent_team").isin("Tech", "Billing", "Onboarding"), F.col("rows_in_agent_team_requester_id_dense_rank"))).alias("rows_in_agent_team_requester_id_dense_rank_agent"),
        F.max("min_requester_id_so_far").alias("max_min_requester_id_so_far"),
        F.min("requester_id_dense_rank_seq").alias("min_requester_id_dense_rank_seq"),
    )
)

result = (
    stats
    .select(
        "requester_id_dense_rank",
        "requester_id_dense_rank_seq",
        "agent_team",
        "total_rows_in_agent_team_requester_id_dense_rank",
        "n_agent_team_in_billing_onboar",
        "agent_team_list",
        "rows_in_agent_team_requester_id_dense_rank_agent",
        "max_min_requester_id_so_far",
        "min_requester_id_dense_rank_seq",
        F.when(F.col("agent_team").isin("VIP", "Billing"), "group_a").when(F.col("agent_team") == "Onboarding", "group_b").otherwise("other").alias("agent_team_group"),
    )
    .orderBy(
        F.col("rows_in_agent_team_requester_id_dense_rank_agent").desc(),
        "requester_id_dense_rank_seq",
        "requester_id_dense_rank",
        "agent_team",
    )
    .limit(20)
)
