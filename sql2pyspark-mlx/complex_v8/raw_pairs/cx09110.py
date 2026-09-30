from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_at_month = Window.partitionBy(F.month("opened_at"))
w_agent_tier = Window.partitionBy("agent_tier")
w_agent_tier_csat_by_csat_ties = Window.partitionBy("agent_tier", "csat").orderBy(F.col("csat").desc())
w_all_by_csat = Window.orderBy(F.col("csat").desc_nulls_first(), "id")
w_all_by_requester_id_moving_2 = Window.orderBy("requester_id", "id").rowsBetween(-2, Window.currentRow)
w_all_by_csat_running = Window.orderBy(F.col("csat").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_tier_agent_team_by_csa = Window.partitionBy("agent_tier", "agent_team").orderBy(F.col("csat").desc(), "id")
w_agent_tier_agent_team_by_csa_2 = Window.partitionBy("agent_tier", "agent_team").orderBy(F.col("csat").desc(), "id").rowsBetween(-3, -1)
w_agent_id_csat = Window.partitionBy("agent_id", "csat")
w_distinct_channel_in_agent_ti = Window.partitionBy("distinct_channel_in_agent_tier").orderBy(F.col("requester_id_avg_3").desc(), "id")
w_prev3_opened_at_month_distin = Window.partitionBy(F.date_trunc("month", "prev3_opened_at"), "distinct_channel_in_agent_tier").orderBy("requester_id_avg_3_prior", "id").rowsBetween(-1, Window.currentRow)
w_prev3_opened_at_month_distin_2 = Window.partitionBy(F.date_trunc("month", "prev3_opened_at"), "distinct_channel_in_agent_tier").orderBy("requester_id_avg_3_prior", "id").rowsBetween(-1, -1)
w_distinct_channel_in_agent_ti_2 = Window.partitionBy("distinct_channel_in_agent_tier").orderBy(F.col("requester_id_avg_3").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_tickets
    .filter(F.col("csat") >= 1)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

combined_details = (
    base.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.channel",
        "s.csat",
        "s.tags",
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
    )
)

metrics = (
    combined_details
    .select(
        "id",
        "agent_id",
        "csat",
        "agent_team",
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_opened_at_month), 2).alias("requester_id_vs_opened_at_month_avg"),
        F.count(F.lit(1)).over(w_agent_tier).alias("rows_in_agent_tier"),
        F.dense_rank().over(w_agent_tier_csat_by_csat_ties).alias("csat_dense_rank"),
        F.lag("opened_at", 3).over(w_all_by_csat).alias("prev3_opened_at"),
        F.round(F.avg("requester_id").over(w_all_by_requester_id_moving_2), 2).alias("requester_id_avg_3"),
        F.size(F.collect_set("channel").over(w_agent_tier)).alias("distinct_channel_in_agent_tier"),
        F.min("csat").over(w_all_by_csat_running).alias("min_csat_so_far"),
        F.rank().over(w_agent_tier_agent_team_by_csa).alias("csat_rank"),
        F.round(F.col("csat") - F.avg("csat").over(w_agent_tier), 2).alias("csat_vs_agent_tier_avg"),
        F.round(F.avg("requester_id").over(w_agent_tier_agent_team_by_csa_2), 2).alias("requester_id_avg_3_prior"),
    )
)

with_trends = (
    metrics
    .select(
        "*",
        F.round(F.col("csat") - F.avg("csat").over(w_agent_id_csat), 2).alias("csat_vs_agent_id_csat_avg"),
        F.row_number().over(w_distinct_channel_in_agent_ti).alias("rn"),
        F.round(F.avg("requester_id_avg_3").over(w_prev3_opened_at_month_distin), 2).alias("requester_id_avg_3_avg_2"),
        F.max("min_csat_so_far").over(w_agent_id_csat).alias("agent_id_csat_max_min_csat_so_far"),
        F.round(F.avg("min_csat_so_far").over(w_prev3_opened_at_month_distin_2), 2).alias("min_csat_so_far_avg_1_prior"),
        F.count(F.lit(1)).over(w_agent_id_csat).alias("rows_in_agent_id_csat"),
        (F.col("csat_rank") - F.lag("csat_rank").over(w_distinct_channel_in_agent_ti)).alias("csat_rank_change"),
        F.first("rows_in_agent_tier").over(w_distinct_channel_in_agent_ti_2).alias("first_rows_in_agent_tier"),
    )
)

result = (
    with_trends
    .filter(F.col("first_rows_in_agent_tier") >= 4)
    .select(
        "id",
        "agent_id",
        "csat",
        "agent_team",
        "requester_id_vs_opened_at_month_avg",
        "csat_dense_rank",
        "requester_id_avg_3",
        "distinct_channel_in_agent_tier",
        "csat_rank",
        "csat_vs_agent_tier_avg",
        "csat_vs_agent_id_csat_avg",
        "rn",
        "requester_id_avg_3_avg_2",
        "agent_id_csat_max_min_csat_so_far",
        "min_csat_so_far_avg_1_prior",
        "csat_rank_change",
        "first_rows_in_agent_tier",
        F.when(F.col("agent_team").isin("Tech", "Onboarding"), "group_a").when(F.col("agent_team") == "VIP", "group_b").otherwise("other").alias("agent_team_group"),
    )
    .orderBy(
        F.col("agent_team").desc_nulls_first(),
        "distinct_channel_in_agent_tier",
        "id",
    )
    .limit(10)
)
