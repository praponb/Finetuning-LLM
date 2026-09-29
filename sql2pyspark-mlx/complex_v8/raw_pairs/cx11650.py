from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_agent_hired_on = Window.orderBy("agent_hired_on", "id")
w_all_by_agent_hired_on_full = Window.orderBy("agent_hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_agent_hired_on_moving = Window.orderBy("agent_hired_on", "id").rowsBetween(-6, Window.currentRow)
w_csat = Window.partitionBy("csat")
w_all_by_csat = Window.orderBy("csat", "id")
w_all_by_csat_ties = Window.orderBy("csat")
w_closed_at_month_by_csat_ties = Window.partitionBy(F.month("closed_at")).orderBy(F.col("csat").desc())
w_all_by_csat_moving_3 = Window.orderBy("csat", "id").rowsBetween(-3, Window.currentRow)
w_all_by_closed_at_prior_3 = Window.orderBy("closed_at", "id").rowsBetween(-3, -1)
w_agent_hired_on_rank_by_csat = Window.partitionBy("agent_hired_on_rank").orderBy(F.col("csat_max_requester_id").desc(), "id")
w_all_by_closed_at = Window.orderBy("closed_at", "id")
w_all_by_closed_at_plus_1d_run = Window.orderBy("closed_at_plus_1d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_hired_on_rank = Window.partitionBy("agent_hired_on_rank")

scoped = (
    df_tickets
    .filter((F.col("channel") == "chat") & (F.month(F.col("opened_at")).isin(1, 2)))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
        F.length(F.trim(F.col("priority"))).alias("priority_len_2"),
    )
)

joined = (
    scoped.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.csat",
        "s.tags",
        "s.priority_len_2",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
        "su.agent_name",
    )
)

with_trends = (
    joined
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "csat",
        "tags",
        "agent_hired_on",
        "agent_name",
        F.rank().over(w_all_by_agent_hired_on).alias("agent_hired_on_rank"),
        F.first("agent_hired_on").over(w_all_by_agent_hired_on_full).alias("first_agent_hired_on"),
        F.max("requester_id").over(w_all_by_agent_hired_on_moving).alias("requester_id_max_7"),
        F.max("requester_id").over(w_csat).alias("csat_max_requester_id"),
        F.lag("priority_len_2").over(w_all_by_agent_hired_on).alias("prev_priority_len_2"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_csat), 2).alias("requester_id_vs_csat_avg"),
        F.ntile(5).over(w_all_by_csat).alias("csat_quintile"),
        F.round(F.cume_dist().over(w_all_by_csat_ties), 3).alias("csat_cume_dist"),
        F.dense_rank().over(w_closed_at_month_by_csat_ties).alias("csat_dense_rank"),
        F.size(F.collect_set("agent_id").over(w_csat)).alias("distinct_agent_id_in_csat"),
        F.round(F.avg("csat").over(w_all_by_csat_moving_3), 2).alias("csat_avg_4"),
        F.min("requester_id").over(w_csat).alias("csat_min_requester_id"),
        F.count(F.lit(1)).over(w_csat).alias("rows_in_csat"),
    )
)

tagged = (
    with_trends
    .select(
        "id",
        "requester_id",
        "closed_at",
        "agent_hired_on",
        "agent_hired_on_rank",
        "first_agent_hired_on",
        "csat_max_requester_id",
        "csat_quintile",
        "csat_cume_dist",
        "csat_dense_rank",
        "distinct_agent_id_in_csat",
        "csat_min_requester_id",
        "rows_in_csat",
        F.concat(F.col("agent_name"), F.lit("#"), F.col("id").cast("string")).alias("agent_name_ref"),
        (F.col("closed_at") + F.expr("INTERVAL 1 DAYS")).alias("closed_at_plus_1d"),
        F.lower(F.regexp_replace(F.trim(F.col("agent_name")), " ", "_")).alias("agent_name_slug"),
    )
)

with_trends_2 = (
    tagged
    .select(
        "*",
        F.round(F.avg("rows_in_csat").over(w_all_by_closed_at_prior_3), 2).alias("rows_in_csat_avg_3_prior"),
        F.lead("csat_min_requester_id", 3).over(w_agent_hired_on_rank_by_csat).alias("next3_csat_min_requester_id"),
        F.lead("requester_id", 3).over(w_all_by_closed_at).alias("next3_requester_id"),
        F.round(F.cume_dist().over(w_agent_hired_on_rank_by_csat), 3).alias("csat_max_requester_id_cume_dist"),
        F.first("csat_dense_rank", ignorenulls=True).over(w_all_by_closed_at_plus_1d_run).alias("first_known_csat_dense_rank"),
        F.lag("csat_dense_rank").over(w_agent_hired_on_rank_by_csat).alias("prev_csat_dense_rank"),
        F.round(F.col("rows_in_csat") - F.avg("rows_in_csat").over(w_agent_hired_on_rank), 2).alias("rows_in_csat_vs_agent_hired_on_rank_avg"),
        F.round(F.col("csat_cume_dist") / F.nullif(F.sum("csat_cume_dist").over(w_agent_hired_on_rank), F.lit(0)), 4).alias("csat_cume_dist_share_of_agent_hired_on_rank"),
    )
)

result = (
    with_trends_2
    .filter(F.col("requester_id") > 8)
    .select(
        "id",
        "requester_id",
        "closed_at",
        "agent_hired_on_rank",
        "first_agent_hired_on",
        "csat_max_requester_id",
        "csat_quintile",
        "distinct_agent_id_in_csat",
        "csat_min_requester_id",
        "rows_in_csat",
        "closed_at_plus_1d",
        "agent_name_slug",
        "next3_csat_min_requester_id",
        "next3_requester_id",
        "csat_max_requester_id_cume_dist",
        "first_known_csat_dense_rank",
        "prev_csat_dense_rank",
        "rows_in_csat_vs_agent_hired_on_rank_avg",
        "csat_cume_dist_share_of_agent_hired_on_rank",
        F.least(F.col("next3_requester_id"), F.col("csat_max_requester_id")).alias("least_next3_requester_id_csat_max_requester_id"),
        F.datediff(F.col("first_agent_hired_on"), F.col("agent_hired_on")).alias("days_agent_hired_on_to_first_agent_hired_on"),
    )
    .orderBy(F.col("days_agent_hired_on_to_first_agent_hired_on").asc())
)
