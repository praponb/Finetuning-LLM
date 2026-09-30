from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_csat = Window.orderBy(F.col("csat").asc_nulls_last(), "id")
w_csat_agent_team = Window.partitionBy("csat", "agent_team")
w_agent_team_agent_id = Window.partitionBy("agent_team", "agent_id")
w_csat_by_hours_opened_at_to_c = Window.partitionBy("csat").orderBy(F.col("hours_opened_at_to_closed_at").desc())
w_csat_by_hours_opened_at_to_c_2 = Window.partitionBy("csat").orderBy(F.col("hours_opened_at_to_closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_hours_opened_at_to_cl = Window.orderBy("hours_opened_at_to_closed_at", "id").rowsBetween(-2, Window.currentRow)
w_all_by_hours_opened_at_to_cl_2 = Window.orderBy("hours_opened_at_to_closed_at")
w_all_by_hours_opened_at_to_cl_3 = Window.orderBy("hours_opened_at_to_closed_at", "id")
w_all_by_hours_opened_at_to_cl_4 = Window.orderBy("hours_opened_at_to_closed_at", "id").rowsBetween(-6, -1)
w_all_by_csat_running = Window.orderBy(F.col("csat").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hours_opened_at_to_closed_at = Window.partitionBy("hours_opened_at_to_closed_at_dense_rank_2", "hours_opened_at_to_closed_at_dense_rank").orderBy("hours_opened_at_to_closed_at", "id")
w_opened_at_month_agent_id_by = Window.partitionBy(F.date_trunc("month", "opened_at"), "agent_id").orderBy(F.col("hours_opened_at_to_closed_at_dense_rank_2").desc(), "id").rowsBetween(-6, -1)
w_hours_opened_at_to_closed_at_2 = Window.partitionBy("hours_opened_at_to_closed_at_dense_rank_2", "hours_opened_at_to_closed_at_dense_rank").orderBy("hours_opened_at_to_closed_at")

recent = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "csat",
        "tags",
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 3600, 1).alias("hours_opened_at_to_closed_at"),
        F.date_format(F.col("opened_at"), "yyyy-MM-dd HH:00").alias("opened_at_label"),
    )
)

combined_details_agent = (
    recent.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.csat",
        "s.tags",
        "s.hours_opened_at_to_closed_at",
        "s.opened_at_label",
        F.col("su.team").alias("agent_team"),
    )
)

metrics = (
    combined_details_agent
    .select(
        "*",
        F.lead("hours_opened_at_to_closed_at", 2, 0).over(w_all_by_csat).alias("next2_hours_opened_at_to_closed_at"),
        F.size(F.collect_set("agent_id").over(w_csat_agent_team)).alias("distinct_agent_id_in_csat_agent_team"),
        F.round(F.col("hours_opened_at_to_closed_at") / F.nullif(F.sum("hours_opened_at_to_closed_at").over(w_agent_team_agent_id), F.lit(0)), 4).alias("hours_opened_at_to_closed_at_share_of_agent_team"),
        F.dense_rank().over(w_csat_by_hours_opened_at_to_c).alias("hours_opened_at_to_closed_at_dense_rank"),
        F.first("csat", ignorenulls=True).over(w_csat_by_hours_opened_at_to_c_2).alias("first_known_csat"),
        F.round(F.avg("csat").over(w_all_by_hours_opened_at_to_cl), 2).alias("csat_avg_3"),
        F.dense_rank().over(w_all_by_hours_opened_at_to_cl_2).alias("hours_opened_at_to_closed_at_dense_rank_2"),
        F.lead("csat", 1, 0).over(w_all_by_csat).alias("next_csat"),
        F.lead("hours_opened_at_to_closed_at", 3, 0).over(w_all_by_hours_opened_at_to_cl_3).alias("next3_hours_opened_at_to_closed_at"),
        F.ntile(4).over(w_all_by_csat).alias("csat_quartile"),
        F.max("hours_opened_at_to_closed_at").over(w_all_by_hours_opened_at_to_cl_4).alias("hours_opened_at_to_closed_at_max_6_prior"),
        F.max("hours_opened_at_to_closed_at").over(w_all_by_csat_running).alias("max_hours_opened_at_to_closed_at_so_far"),
    )
)

top_per_group = metrics.filter(F.col("hours_opened_at_to_closed_at_dense_rank_2") == 1)

metrics_2 = (
    top_per_group
    .select(
        "*",
        F.round(F.cume_dist().over(w_hours_opened_at_to_closed_at), 3).alias("hours_opened_at_to_closed_at_cume_dist"),
        F.sum("next_csat").over(w_opened_at_month_agent_id_by).alias("next_csat_sum_6_prior"),
        F.round(F.percent_rank().over(w_hours_opened_at_to_closed_at_2), 3).alias("hours_opened_at_to_closed_at_percent_rank"),
    )
)

result = (
    metrics_2
    .filter(F.col("opened_at_label").isNotNull())
    .select(
        "id",
        "agent_id",
        "opened_at",
        "csat",
        "tags",
        "opened_at_label",
        "agent_team",
        "next2_hours_opened_at_to_closed_at",
        "distinct_agent_id_in_csat_agent_team",
        "hours_opened_at_to_closed_at_dense_rank",
        "first_known_csat",
        "csat_avg_3",
        "next3_hours_opened_at_to_closed_at",
        "hours_opened_at_to_closed_at_max_6_prior",
        "max_hours_opened_at_to_closed_at_so_far",
        "hours_opened_at_to_closed_at_cume_dist",
        "hours_opened_at_to_closed_at_percent_rank",
        F.concat_ws("|", F.col("opened_at_label"), F.col("agent_team")).alias("opened_at_label_agent_team_label"),
    )
    .orderBy(F.col("opened_at").asc(), F.col("agent_id").asc_nulls_last())
)
