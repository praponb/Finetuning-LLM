from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_requester_id_prior_1 = Window.orderBy("requester_id", "id").rowsBetween(-1, -1)
w_agent_id_agent_team_by_reque = Window.partitionBy("agent_id", "agent_team").orderBy("requester_id", "id")
w_csat_agent_team_by_requester = Window.partitionBy("csat", "agent_team").orderBy("requester_id", "id")
w_agent_tier = Window.partitionBy("agent_tier")
w_closed_at_to_date_agent_team = Window.partitionBy(F.to_date("closed_at"), "agent_team")
w_csat = Window.partitionBy("csat")
w_csat_agent_team_by_requester_2 = Window.partitionBy("csat", "agent_team").orderBy("requester_id", "id").rowsBetween(-3, Window.currentRow)
w_all_by_closed_at_to_date_age = Window.orderBy("closed_at_to_date_agent_team_max_requester_id", "id")
w_all_by_closed_at_to_date_age_2 = Window.orderBy("closed_at_to_date_agent_team_max_requester_id", "id").rowsBetween(-6, -1)
w_csat_requester_id_dense_rank = Window.partitionBy("csat", "requester_id_dense_rank")
w_requester_id_dense_rank_dist = Window.partitionBy("requester_id_dense_rank", "distinct_agent_id_in_agent_tier").orderBy(F.col("next2_requester_id").asc_nulls_last(), "id")

base = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "csat",
        "tags",
    )
)

with_parent_agent = (
    base.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.tier").isin("T3", "T1")), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.csat",
        "s.tags",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

with_trends = (
    with_parent_agent
    .select(
        "*",
        F.max("csat").over(w_all_by_requester_id_prior_1).alias("csat_max_1_prior"),
        F.lead("requester_id", 2).over(w_agent_id_agent_team_by_reque).alias("next2_requester_id"),
        F.lag("requester_id", 3, 0).over(w_csat_agent_team_by_requester).alias("prev3_requester_id"),
        F.size(F.collect_set("agent_id").over(w_agent_tier)).alias("distinct_agent_id_in_agent_tier"),
        F.max("requester_id").over(w_closed_at_to_date_agent_team).alias("closed_at_to_date_agent_team_max_requester_id"),
        F.dense_rank().over(w_agent_id_agent_team_by_reque).alias("requester_id_dense_rank"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_csat), F.lit(0)), 4).alias("requester_id_share_of_csat"),
        F.round(F.avg("requester_id").over(w_csat_agent_team_by_requester_2), 2).alias("requester_id_avg_4"),
    )
)

first_rows = (
    with_trends
    .filter((F.col("requester_id_dense_rank") <= 5) & (F.col("closed_at_to_date_agent_team_max_requester_id").between(63, 188)))
)

with_windows = (
    first_rows
    .select(
        "*",
        F.row_number().over(w_all_by_closed_at_to_date_age).alias("seq_no"),
        F.round(F.avg("next2_requester_id").over(w_all_by_closed_at_to_date_age_2), 2).alias("next2_requester_id_avg_6_prior"),
        (F.col("next2_requester_id") - F.lag("next2_requester_id").over(w_all_by_closed_at_to_date_age)).alias("next2_requester_id_change"),
        F.round(F.col("next2_requester_id") - F.avg("next2_requester_id").over(w_csat_requester_id_dense_rank), 2).alias("next2_requester_id_vs_csat_requester_id_dense_ra"),
        (F.col("requester_id_avg_4") - F.lag("requester_id_avg_4").over(w_requester_id_dense_rank_dist)).alias("requester_id_avg_4_change"),
    )
)

result = (
    with_windows
    .filter(F.col("agent_team") == "Tech")
    .select(
        "id",
        "requester_id",
        "agent_id",
        "csat",
        "tags",
        "agent_tier",
        "agent_team",
        "csat_max_1_prior",
        "next2_requester_id",
        "prev3_requester_id",
        "distinct_agent_id_in_agent_tier",
        "closed_at_to_date_agent_team_max_requester_id",
        "requester_id_dense_rank",
        "requester_id_share_of_csat",
        "requester_id_avg_4",
        "seq_no",
        "next2_requester_id_avg_6_prior",
        "next2_requester_id_change",
        "next2_requester_id_vs_csat_requester_id_dense_ra",
        "requester_id_avg_4_change",
        F.lower(F.trim(F.col("agent_tier"))).alias("agent_tier_clean"),
        F.substring(F.trim(F.col("agent_tier")), 1, 3).alias("agent_tier_prefix"),
    )
    .orderBy(F.col("agent_tier_prefix").desc(), "next2_requester_id_change", "id")
    .limit(20)
)
