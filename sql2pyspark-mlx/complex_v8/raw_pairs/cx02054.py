from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_csat = Window.partitionBy("agent_id", "csat")
w_agent_tier = Window.partitionBy("agent_tier")
w_agent_id_agent_team = Window.partitionBy("agent_id", "agent_team")
w_agent_team_by_requester_id_t = Window.partitionBy("agent_team").orderBy("requester_id")
w_all_by_csat_moving_3 = Window.orderBy("csat", "id").rowsBetween(-3, Window.currentRow)
w_all_by_csat = Window.orderBy("csat", "id")
w_agent_team_by_requester_id_m = Window.partitionBy("agent_team").orderBy("requester_id", "id").rowsBetween(-1, Window.currentRow)
w_agent_team_by_requester_id_r = Window.partitionBy("agent_team").orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_tier_by_requester_id_f = Window.partitionBy("agent_tier").orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_csat_prior_2 = Window.orderBy("csat", "id").rowsBetween(-2, -1)
w_distinct_agent_id_in_agent_t = Window.partitionBy("distinct_agent_id_in_agent_tier").orderBy(F.col("running_csat").asc_nulls_last())
w_requester_id_rank = Window.partitionBy("requester_id_rank")
w_distinct_agent_id_in_agent_t_2 = Window.partitionBy("distinct_agent_id_in_agent_tier").orderBy(F.col("running_csat").asc_nulls_last(), "id")
w_requester_id_rank_csat_rank = Window.partitionBy("requester_id_rank", "csat_rank").orderBy(F.col("csat_vs_agent_id_agent_team_avg").desc())

scoped_tickets = (
    df_tickets
    .filter(F.array_contains(F.col("tags"), "login"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "csat",
        F.initcap(F.trim(F.col("priority"))).alias("priority_clean"),
    )
)

with_parent = (
    scoped_tickets.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (~F.col("su.tier").isin("T2", "T1")), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.csat",
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
    )
)

metrics = (
    with_parent
    .select(
        "id",
        "requester_id",
        "csat",
        "agent_team",
        "agent_tier",
        F.round(F.col("csat") - F.avg("csat").over(w_agent_id_csat), 2).alias("csat_vs_agent_id_csat_avg"),
        F.size(F.collect_set("agent_id").over(w_agent_tier)).alias("distinct_agent_id_in_agent_tier"),
        F.sum("requester_id").over(w_agent_id_agent_team).alias("agent_id_agent_team_sum_requester_id"),
        F.rank().over(w_agent_team_by_requester_id_t).alias("requester_id_rank"),
        F.round(F.avg("csat").over(w_all_by_csat_moving_3), 2).alias("csat_avg_4"),
        F.rank().over(w_all_by_csat).alias("csat_rank"),
        F.max("csat").over(w_agent_team_by_requester_id_m).alias("csat_max_2"),
        F.max("csat").over(w_agent_tier).alias("agent_tier_max_csat"),
        F.sum("csat").over(w_agent_team_by_requester_id_r).alias("running_csat"),
        F.round(F.col("csat") - F.avg("csat").over(w_agent_id_agent_team), 2).alias("csat_vs_agent_id_agent_team_avg"),
        F.nth_value("requester_id", 2).over(w_agent_tier_by_requester_id_f).alias("second_requester_id"),
        F.count(F.lit(1)).over(w_agent_id_agent_team).alias("rows_in_agent_id_agent_team"),
        F.round(F.avg("requester_id").over(w_all_by_csat_prior_2), 2).alias("requester_id_avg_2_prior"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_agent_id_agent_team), 2).alias("requester_id_vs_agent_id_agent_team_avg"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_agent_tier), 2).alias("requester_id_vs_agent_tier_avg"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_agent_id_csat), F.lit(0)), 4).alias("requester_id_share_of_agent_id_csat"),
    )
)

tagged = (
    metrics
    .filter(F.col("requester_id_vs_agent_tier_avg") > -5.0)
    .select(
        "*",
        F.greatest(F.col("requester_id"), F.col("csat_rank")).alias("greatest_requester_id_csat_rank"),
        F.upper(F.trim(F.col("agent_tier"))).alias("agent_tier_clean"),
        F.coalesce(F.col("csat_avg_4"), F.lit(0.0)).alias("csat_avg_4_filled"),
    )
)

with_trends = (
    tagged
    .select(
        "*",
        F.round(F.percent_rank().over(w_distinct_agent_id_in_agent_t), 3).alias("running_csat_percent_rank"),
        F.round(F.col("csat_avg_4") - F.avg("csat_avg_4").over(w_requester_id_rank), 2).alias("csat_avg_4_vs_requester_id_rank_avg"),
        F.round(F.cume_dist().over(w_distinct_agent_id_in_agent_t_2), 3).alias("running_csat_cume_dist"),
        F.round(F.percent_rank().over(w_requester_id_rank_csat_rank), 3).alias("csat_vs_agent_id_agent_team_avg_percent_rank"),
        F.round(F.col("second_requester_id") - F.avg("second_requester_id").over(w_requester_id_rank), 2).alias("second_requester_id_vs_requester_id_rank_avg"),
    )
)

rollup_base = (
    with_trends
    .groupBy("distinct_agent_id_in_agent_tier")
    .agg(
        F.max("requester_id_rank").alias("max_requester_id_rank"),
        F.sum("second_requester_id_vs_requester_id_rank_avg").alias("total_second_requester_id_vs_requester_id_rank_a"),
        F.count("*").alias("cnt"),
        F.min("second_requester_id_vs_requester_id_rank_avg").alias("min_second_requester_id_vs_requester_id_rank_avg"),
    )
)

result = (
    rollup_base
    .filter(F.col("total_second_requester_id_vs_requester_id_rank_a") >= 45.0)
    .select(
        "distinct_agent_id_in_agent_tier",
        "total_second_requester_id_vs_requester_id_rank_a",
        "cnt",
        "min_second_requester_id_vs_requester_id_rank_avg",
        F.round(F.col("max_requester_id_rank") / F.nullif(F.col("cnt"), F.lit(0)), 3).alias("max_requester_id_rank_per_cnt"),
    )
    .orderBy(F.col("max_requester_id_rank_per_cnt").desc(), F.col("cnt").desc())
)
