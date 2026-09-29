from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_closed_at_to_date_b = Window.partitionBy("agent_id", F.to_date("closed_at")).orderBy("opened_at", "id").rowsBetween(-3, Window.currentRow)
w_all_by_closed_at_ties = Window.orderBy(F.col("closed_at").desc())
w_agent_team_by_closed_at_runn = Window.partitionBy("agent_team").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_closed_at_prior_2 = Window.orderBy(F.col("closed_at").desc(), "id").rowsBetween(-2, -1)
w_priority_csat = Window.partitionBy("priority", "csat")
w_agent_id_closed_at_to_date_b_2 = Window.partitionBy("agent_id", F.to_date("closed_at")).orderBy("opened_at", "id")
w_agent_id_closed_at_to_date_b_3 = Window.partitionBy("agent_id", F.to_date("closed_at")).orderBy("opened_at", "id").rowsBetween(-1, Window.currentRow)
w_priority_opened_at_year_by_a = Window.partitionBy("priority", F.year("opened_at")).orderBy(F.col("agent_hired_on").desc(), "id").rowsBetween(-1, -1)
w_agent_id_closed_at_to_date_b_4 = Window.partitionBy("agent_id", F.to_date("closed_at")).orderBy("opened_at", "id").rowsBetween(-1, -1)
w_priority_opened_at_year_by_a_2 = Window.partitionBy("priority", F.year("opened_at")).orderBy(F.col("agent_hired_on").desc(), "id")
w_closed_at_rank_priority_by_c = Window.partitionBy("closed_at_rank", "priority").orderBy("csat")
w_csat = Window.partitionBy("csat")
w_all_by_distinct_agent_team_i = Window.orderBy("distinct_agent_team_in_priority_csat", "id")
w_all_by_distinct_agent_team_i_2 = Window.orderBy("distinct_agent_team_in_priority_csat")
w_closed_at_rank_priority_by_c_2 = Window.partitionBy("closed_at_rank", "priority").orderBy("csat", "id")

filtered = (
    df_tickets
    .filter((F.col("requester_id") <= 22) & (F.to_date(F.col("closed_at")) < F.lit("2025-03-22").cast("date")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
        F.concat(F.col("channel"), F.lit("#"), F.col("requester_id").cast("string")).alias("channel_ref"),
    )
)

combined_details = (
    filtered.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.hired_on").isNotNull())
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.csat",
        "s.channel_ref",
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

sequenced = (
    combined_details
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "channel_ref",
        "agent_team",
        F.max("csat").over(w_agent_id_closed_at_to_date_b).alias("csat_max_4"),
        F.sum("csat").over(w_agent_id_closed_at_to_date_b).alias("csat_sum_4"),
        F.rank().over(w_all_by_closed_at_ties).alias("closed_at_rank"),
        F.first("opened_at", ignorenulls=True).over(w_agent_team_by_closed_at_runn).alias("first_known_opened_at"),
        F.round(F.avg("csat").over(w_all_by_closed_at_prior_2), 2).alias("csat_avg_2_prior"),
        F.min("csat").over(w_priority_csat).alias("priority_csat_min_csat"),
        F.round(F.percent_rank().over(w_all_by_closed_at_ties), 3).alias("closed_at_percent_rank"),
        F.lead("csat").over(w_agent_id_closed_at_to_date_b_2).alias("next_csat"),
        F.round(F.avg("csat").over(w_agent_id_closed_at_to_date_b_3), 2).alias("csat_avg_2"),
        F.max("csat").over(w_priority_opened_at_year_by_a).alias("csat_max_1_prior"),
        F.round(F.avg("csat").over(w_agent_id_closed_at_to_date_b_4), 2).alias("csat_avg_1_prior"),
        F.max("csat").over(w_priority_csat).alias("priority_csat_max_csat"),
        F.size(F.collect_set("agent_team").over(w_priority_csat)).alias("distinct_agent_team_in_priority_csat"),
        F.lead("agent_hired_on", 3).over(w_priority_opened_at_year_by_a_2).alias("next3_agent_hired_on"),
    )
)

tagged = (
    sequenced
    .filter(F.col("distinct_agent_team_in_priority_csat") > 5)
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
        "channel_ref",
        "agent_team",
        "csat_sum_4",
        "closed_at_rank",
        "closed_at_percent_rank",
        "csat_avg_2",
        "csat_max_1_prior",
        "csat_avg_1_prior",
        "priority_csat_max_csat",
        "distinct_agent_team_in_priority_csat",
        "next3_agent_hired_on",
        F.abs(F.col("csat_avg_2") - 3.8).alias("csat_avg_2_distance"),
    )
)

sequenced_2 = (
    tagged
    .select(
        "*",
        F.round(F.percent_rank().over(w_closed_at_rank_priority_by_c), 3).alias("csat_percent_rank"),
        F.sum("csat_avg_2_distance").over(w_csat).alias("csat_sum_csat_avg_2_distance"),
        F.row_number().over(w_all_by_distinct_agent_team_i).alias("seq_no"),
        F.dense_rank().over(w_closed_at_rank_priority_by_c).alias("csat_dense_rank"),
        F.round(F.col("csat_max_1_prior") - F.avg("csat_max_1_prior").over(w_csat), 2).alias("csat_max_1_prior_vs_csat_avg"),
        F.round(F.percent_rank().over(w_all_by_distinct_agent_team_i_2), 3).alias("distinct_agent_team_in_priority_csat_percent_ran"),
        F.lead("csat_sum_4").over(w_closed_at_rank_priority_by_c_2).alias("next_csat_sum_4"),
    )
)

result = (
    sequenced_2
    .filter(F.hour(F.col("closed_at")).between(8, 14))
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel_ref",
        "agent_team",
        "closed_at_rank",
        "closed_at_percent_rank",
        "csat_avg_2",
        "csat_max_1_prior",
        "csat_avg_1_prior",
        "priority_csat_max_csat",
        "distinct_agent_team_in_priority_csat",
        "next3_agent_hired_on",
        "csat_avg_2_distance",
        "csat_percent_rank",
        "csat_sum_csat_avg_2_distance",
        "csat_max_1_prior_vs_csat_avg",
        "distinct_agent_team_in_priority_csat_percent_ran",
        (F.floor(F.col("csat_sum_4") / 2) * 2).alias("csat_sum_4_bucket"),
    )
    .orderBy("channel_ref", F.col("distinct_agent_team_in_priority_csat").asc(), "id")
    .limit(5)
)
